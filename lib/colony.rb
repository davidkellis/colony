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
      key
    end
    
    def tube_name
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
    
    def enqueue_callback(tube)
      callback_task = SimpleTask.create(fn: self.callback, args: [self.full_id, self.result_uri])
      callback_task.pool = @pool
      message_id = callback_task.enqueue(tube)
    end
    
    def enqueue_notification
      @pool.use(self.tube_name)
      @pool.yput(self)
    end
    
    # The default timeout is "forever", i.e. wait indefinitely.
    def value(timeout = nil)
      return @result if @result
      
      poll_for_result
      
      # listen for task completion notification if the task is set up to do that
      if notify && @pool
        @pool.watch(tube_name)           # subscribe to the task-specific tube
        
        begin
          
          # if we already have a result_uri, just clear any message off the queue
          if result_uri
            if @pool.peek_ready
              msg = @pool.reserve(0)        # poll the queue for a pre-existing message. Timeout immediately if there is no message.
              msg.delete if msg             # delete the message, we don't need it
            end
          else      # we don't already have a result_uri, so listen for one
            msg = @pool.reserve(timeout)    # listen for a message; timeout if there is no message within the given timeout period.
            if msg
              msg.delete                    # remove the message from the queue
              
              notification_task = msg.ybody
              
              @result = notification_task.download_result
            end
          end
        
        # rescue Beanstalk::TimedOut => e
        #   retry
        ensure
          @pool.ignore(tube_name)       # stop watching the queue
        end
      elsif result_uri.nil?     # we're not setup to listen to a queue for task-completion, so poll for a result if we don't already have one
        if timeout              # we need to poll for a result for timeout seconds
          timeout.to_i.times do
            sleep(1)            # sleep for one second
            poll_for_result     # poll for a result
            break if @result    # break out if we finally have a result
          end
        else
          loop do
            sleep(1)            # sleep for one second
            poll_for_result     # poll for a result
            break if @result    # break out if we finally have a result
          end
        end
      end
      
      @result
    end
    
    def poll_for_result
      # reload the task's result_uri from "global" redis
      reload!(:result_uri)

      # download the task's result if the task's result_uri field is set
      download_result
    end
    
    # download the task's result if the task's result_uri field is set
    # This method returns the downloaded result, or nil if result_uri isn't set.
    # The downloaded result is also assigned to this task's @result instance variable.
    def download_result
      return @result if @result

      if result_uri
        uri = LegacyExtendedIRI.new(result_uri)       # uri will be something like this: "redis://127.0.0.1:6379/0/12345"
        db, result_id = uri.path.split('/').drop(1)
      
        redis = Redis.new(:host => uri.host || "127.0.0.1", :port => uri.port || 6379, :db => db || 0)
        @result = redis.get(result_id)
        redis.quit
        
        @result
      end
    end
  end
  
  class SimpleTask < Ohm::Model
    include TaskMessage
    
    def initialize(attrs = {})
      super(attrs.merge(type: Message::SIMPLE_TASK))
    end
  end
  
  class Job < Ohm::Model
    include Message
    
    attribute :task_count
    attribute :status
    counter :completed_task_count
    collection :tasks, JobTask
    
    def initialize(attrs = {})
      super(attrs.merge(type: Message::JOB))
    end
    
    def task(fn_name, args_array, notify_on_complete = false, callback_fn = nil)
      task = JobTask.create(status: States::NEW, 
                            fn: fn_name, 
                            args: args_array, 
                            callback: callback_fn, 
                            notify: notify_on_complete,
                            job: self)
      task.pool = self.pool
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
  end
  
  class JobTask < Ohm::Model
    include TaskMessage
    
    reference :job, Job
    
    def initialize(attrs = {})
      super(attrs.merge(type: Message::JOB_TASK))
    end
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
    
    def job(notify_on_complete = false, callback_fn = nil)
      j = Job.create(status: States::NEW, callback: callback_fn, notify: notify_on_complete)
      j.pool = @pool
      j
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
    
    # The task.fn must either reference a top-level function/method or it must
    # be in dotted form, s.t. the substring to the right of the right-most period is the method name
    # and the substring to the left of the right-most period is the object on which to invoke the method.
    def invoke(task)
      fn = task.fn.to_s
      i = fn.rindex('.')
      if i
        obj = fn[0, i]
        method = fn[i + 1, fn.length - (i + 1)]
        eval(obj).send(method, *task.args)
      else
        TOPLEVEL.send(fn, *task.args)
      end
    end
    
    # stores the result in mongodb and returns the document id
    def store_result(result)
      result_id = UUIDTools::UUID.random_create.to_s
      @local_redis_server.set(result_id, result)
      
      result_uri = "redis://#{@local_redis_server.client.host}:#{@local_redis_server.client.port}/#{@local_redis_server.client.db}/#{result_id}"
      result_uri
    end
  end
end