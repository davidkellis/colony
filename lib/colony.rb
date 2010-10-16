# require 'mongo'
# require 'mongoid'
require 'redis'
require 'ohm'
require 'beanstalk-client'
require 'leiri'
require 'uuidtools'

TOPLEVEL = self

module Colony
  # status states
  module States
    
    # task states
    NEW = :new
    QUEUED = :queued
    NOT_QUEUED = :notqueued
    RUNNING = :running
    CALLBACK = :callback
    COMPLETE = :done
    UNKNOWN = :unknown
  end
  
  module Tubes
    DEFAULT = :default
    NEW_TASK = :colony_new
    STATUS = :colony_status
  end
  
  module Message
    # Message types
    SIMPLE_TASK = :task
    JOB_TASK = :jobtask
    JOB = :job

    STATUS_QUERY = :statusq
    STATUS = :status
    INFO = :info
    RESULT = :result
    
    def self.included(mod)
      mod.module_eval do
        attribute :type
        
        # the pool attribute is an instance of Beanstalk::Pool
        attr_accessor :pool
      end
    end

    # TODO: keep this, or use the default id generation?
    def initialize_id
      @id ||= UUIDTools::UUID.random_create.to_s
    end
    
    def enqueue(tube, bs_pool = nil)
      @pool = bs_pool if bs_pool
      
      @pool.use(tube)
      @pool.yput(self)
    end
    
    def to_hash
      super.merge(type: type)
    end
    
    def full_id
      key.gsub(":", "-")
    end
  end
  
  module TaskMessage
    def self.included(mod)
      mod.module_eval do
        include Message
        
        attribute :fn
        attribute :args
        attribute :callback
        attribute :result_uri
        attribute :notify
        attribute :status
      end
    end
    
    # bs_pool is a Beanstalk::Pool object
    def enqueue_callback(tube, bs_pool = nil)
      @pool = bs_pool if bs_pool
      
      callback_task = SimpleTask.create(fn: self.callback, args: [self.full_id, self.result_uri])
      callback_task.pool = @pool
      message_id = callback_task.enqueue(tube)
    end
    
    # bs_pool is a Beanstalk::Pool object
    def enqueue_notification(bs_pool = nil)
      @pool = bs_pool if bs_pool
      
      @pool.use(self.full_id)
      @pool.yput(self)
    end
    
    def result_reference
      @result_ref ||= ResultReference.new(self, @pool)
    end
    
    def value(timeout = nil)
      result_reference.value(timeout)
    end
    
    # def to_hash
    #   super.merge(fn: fn, args: args, callback: callback)
    # end
  end
  
  class SimpleTask < Ohm::Model
    include TaskMessage
    
    def initialize(attrs = {})
      super(attrs.merge(type: Message::SIMPLE_TASK))
    end
  end
  
  class Job < Ohm::Model
    include Message
    
    collection :tasks, JobTask
    
    def initialize(attrs = {})
      super(attrs.merge(type: Message::JOB))
    end
  end
  
  class JobTask < Ohm::Model
    include TaskMessage
    
    attribute :job_id
    
    def initialize(attrs = {})
      super(attrs.merge(type: Message::JOB_TASK))
    end
    
    # def to_hash
    #   super.merge(job_id: job_id)
    # end
  end
  
  class Client
    def initialize(queues, global_redis_server = {})
      @pool = Beanstalk::Pool.new(queues)
      # @pool.ignore(TUBE_DEFAULT)
      
      Ohm.connect(global_redis_server)
    end
    
    def task(fn_name, args_array, notify_on_complete = false, callback_fn = nil)
      task = SimpleTask.create(status: States::NEW, 
                               fn: fn_name, 
                               args: args_array, 
                               callback: callback_fn, 
                               notify: notify_on_complete)
      task.pool = @pool
      message_id = task.enqueue(Tubes::NEW_TASK)
      
      # if the response was a numeric job id, then the task was successfully enqueued
      if message_id.is_a? Numeric
        task.status = States::QUEUED
      else
        # try to enqueue the task again
        if task.enqueue(Tubes::NEW_TASK).is_a? Numeric
          task.status = States::QUEUED
        else
          task.status = States::NOT_QUEUED
        end
      end
      
      task
    end
    
    # def job(tasks)
    # end
  end
  
  # This class is not thread-safe. Only one ResultReference object per Client object
  # should be used at once. It is safe to call ResultReference#value from different ResultReference objects
  # in different threads *as long as* they were generated from different Client objects.
  class ResultReference
    def initialize(task, mq_pool = nil)
      @task = task
      @pool = mq_pool
    end
    
    # download the task's result if the task's result_uri field is set
    def download_result(task = nil)
      return @result if @result

      task ||= @task
      
      if task.result_uri
        uri = LegacyExtendedIRI.new(task.result_uri)       # uri will be something like this: "redis://127.0.0.1:6379/0/12345"
        db, result_id = uri.path.split('/').drop(1)
      
        redis = Redis.new(:host => uri.host || "127.0.0.1", :port => uri.port || 6379, :db => db || 0)
        @result = redis.get(result_id)
        redis.quit
        
        @result
      end
    end
    
    # The default timeout is "forever", i.e. wait indefinitely.
    def value(timeout = nil)
      return @result if @result
      
      # reload the task's result_uri from "global" redis
      @task.reload!(:result_uri)
      
      # download the task's result if the task's result_uri field is set
      download_result
      
      # listen for task completion notification if the task is set up to do that
      if @task.notify && @pool
        @pool.watch(@task.full_id)           # subscribe to the task-specific tube
        
        begin
          
          # if we already have a result_uri, just clear any message off the queue
          if @task.result_uri
            if @pool.peek_ready
              msg = @pool.reserve(0)        # poll the queue for a pre-existing message. Timeout immediately if there is no message.
              msg.delete if msg             # delete the message, we don't need it
            end
          else      # we don't already have a result_uri, so listen for one
            msg = @pool.reserve(timeout)    # listen for a message; timeout if there is no message within the given timeout period.
            if msg
              msg.delete                    # remove the message from the queue

              notification_task = msg.ybody
            
              download_result(notification_task)
            end
          end
        
        # rescue Beanstalk::TimedOut => e
        #   retry
        ensure
          @pool.ignore(@task.full_id)       # stop watching the queue
        end
      end
      
      @result
    end
  end
  
  class Worker
    def initialize(queues, global_redis_server = {}, local_redis_server = {})
      @pool = Beanstalk::Pool.new(queues)
      # @pool.ignore(TUBE_DEFAULT)
      @pool.watch(Tubes::NEW_TASK)
      
      Ohm.connect(global_redis_server)
      
      @local_redis_server = Redis.new(local_redis_server)
    end
    
    def start
      loop do
        job = @pool.reserve
        
        task = job.ybody
        
        task.pool = @pool
        
        task.update(status: States::RUNNING)
        
        result = invoke(task)
        
        result_uri = store_result(result)
        
        task.update(result_uri: result_uri, status: States::COMPLETE)
        
        if task.callback
          task.enqueue_callback(Tubes::NEW_TASK)
        end
        
        if task.notify
          task.enqueue_notification
        end
        
        job.delete    # mark the job as complete
      end
    end
    
    def invoke(task)
      TOPLEVEL.send(task.fn, *task.args)
    end
    
    # stores the result in mongodb and returns the document id
    def store_result(result)
      result_id = UUIDTools::UUID.random_create.to_s
      @local_redis_server.set(result_id, result)
      
      # result_uri = "mongodb://#{host}:#{port}/#{db_name}/#{coll_name}/#{result_id}"
      result_uri = "redis://#{@local_redis_server.client.host}:#{@local_redis_server.client.port}/#{@local_redis_server.client.db}/#{result_id}"
      result_uri
    end
  end
end