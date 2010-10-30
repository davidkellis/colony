require 'redis'
require 'beanstalk-client'
require 'leiri'
require 'redismodel'
require 'uuidtools'
require 'yaml'

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

  # Message classes that include Message must also include RedisModel
  # because Message calls key_prefix, a method defined in RedisModel.
  module Message
    def self.included(mod)
      mod.class_eval do
        # the pool attribute is an instance of Beanstalk::Pool
        attr_accessor :pool
      end
    end
    
    def enqueue(tube)
      pool.use(tube)
      pool.yput(self)
    end
    
    def full_id
      key_prefix      # a string like this: "TaskMessage:e9601b8e-79cd-4d48-b6ba-81acc5592bea"
    end
    
    def tube_name
      key_prefix.gsub(':','-')    # # a string like this: "TaskMessage-e9601b8e-79cd-4d48-b6ba-81acc5592bea"
    end
  end
  
  # All TaskMessage classes have 6 fields: fn, args, callback, result_uri, notify, and status
  # Classes that include TaskMessage must include Message before including TaskMessage.
  module TaskMessage
    def self.included(mod)
      mod.class_eval do
        field :fn
        field :args
        field :status
        field :callback
        field :notify
        field :result_uri
      end
    end
    
    def marshal_fn
      YAML.dump(fn)
    end
    
    def marshal_args
      YAML.dump(args)
    end
    
    def marshal_status
      YAML.dump(status)
    end
    
    def marshal_callback
      YAML.dump(callback)
    end
    
    def marshal_notify
      YAML.dump(notify)
    end
    
    def unmarshal_fn(stored_value)
      YAML.load(stored_value)
    end
    
    def unmarshal_args(stored_value)
      YAML.load(stored_value)
    end
    
    def unmarshal_status(stored_value)
      YAML.load(stored_value)
    end
    
    def unmarshal_callback(stored_value)
      YAML.load(stored_value)
    end
    
    def unmarshal_notify(stored_value)
      YAML.load(stored_value)
    end
    
    def enqueue(tube = Tubes::NEW_TASK)
      super(tube)
    end
    
    def enqueue_callback(tube = Tubes::NEW_TASK)
      if callback && callback.length > 0
        callback_task = SimpleTask.create(redis, fn: callback, args: [self.full_id, self.result_uri], pool: pool)
        message_id = callback_task.enqueue(tube)
      end
    end
    
    def enqueue_notification
      if notify
        pool.use(tube_name)
        pool.yput(self)
      end
    end
    
    # The default timeout is "forever", i.e. wait indefinitely.
    def value(timeout = nil)
      return @result if @result
      
      poll_for_result
      
      if notify && pool       # listen for task completion notification if the task is set up to do that
        listen_for_result_notification(timeout)
      elsif result_uri.nil?   # wait for a result if we don't already have one
        wait_for_result(timeout)
      end
      
      @result
    end
    
    def poll_for_result
      # reload the task's result_uri from "global" redis
      reload!([:result_uri])

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
    
    def listen_for_result_notification(timeout)
      pool.watch(tube_name)           # subscribe to the task-specific tube
      
      begin
        # if we already have a result_uri, just clear any message off the queue
        if result_uri
          if pool.peek_ready
            msg = pool.reserve(0)        # poll the queue for a pre-existing message. Timeout immediately if there is no message.
            msg.delete if msg             # delete the message, we don't need it
          end
        else      # we don't already have a result_uri, so listen for one
          msg = pool.reserve(timeout)    # listen for a message; timeout if there is no message within the given timeout period.
          if msg
            msg.delete                    # remove the message from the queue
            
            notification_task = msg.ybody
            
            @result = notification_task.download_result   # NOTE: we assume that the notification task has a valid result_id
          end
        end
      
      # rescue Beanstalk::TimedOut => e
      #   retry
      ensure
        pool.ignore(tube_name)       # stop watching the queue
      end
    end
    
    def wait_for_result(timeout)
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
  end
  
  class SimpleTask
    include RedisModel
    include Message
    include TaskMessage
  end
  
  class JobTask
    include RedisModel
    include Message
    include TaskMessage
    
    belongs_to :job, 'Colony::Job', :tasks
  end
  
  class JobReference
    attr_accessor :id, :status
    
    def initialize(job)
      self.id = job.id
      self.status = job.status
    end
  end
  
  class Job
    include RedisModel
    include Message
    
    field :status
    field :callback
    field :notify
    field :task_count
    field :completed_task_count
    
    has_many :tasks, 'Colony::JobTask'
    
    def marshal_status
      YAML.dump(status)
    end
    
    def marshal_callback
      YAML.dump(callback)
    end
    
    def marshal_notify
      YAML.dump(notify)
    end
    
    def unmarshal_status(stored_value)
      YAML.load(stored_value)
    end
    
    def unmarshal_callback(stored_value)
      YAML.load(stored_value)
    end
    
    def unmarshal_notify(stored_value)
      YAML.load(stored_value)
    end
    
    # the "marshalled" version of task_count should simply be a string, so just convert it to an integer
    def unmarshal_task_count(stored_value)
      stored_value.to_i
    end

    # the "marshalled" version of completed_task_count should simply be a string, so just convert it to an integer
    def unmarshal_completed_task_count(stored_value)
      stored_value.to_i
    end
    
    # do the job-completion tasks if the job's subtasks are all complete
    def increment_completed_task_count
      new_completed_count = increment(:completed_task_count)
      
      # the job is complete if the completed_task_count == the total task count
      if new_completed_count == task_count
        mark_complete!
        enqueue_callback
        enqueue_notification
      end
      
      new_completed_count
    end
    
    def mark_complete!
      update!(status: States::COMPLETE)      # mark the job as complete
    end
    
    # call when all the job's tasks are complete
    def enqueue_callback(tube = Tubes::NEW_TASK)
      if callback && callback.length > 0
        callback_task = SimpleTask.create(redis, fn: callback, args: [full_id, task_id_and_result_uri_pairs])
        callback_task.pool = @pool
        message_id = callback_task.enqueue(tube)
      end
    end
    
    def task_id_and_result_uri_pairs
      tasks.map {|t| [t.full_id, t.result_uri] }
    end
    
    def enqueue_notification
      if notify
        notification = JobReference.new(self)
        pool.use(self.tube_name)
        pool.yput(notification)
      end
    end
    
    def complete?(reload = true)
      reload!([:status]) if reload
      (status == States::COMPLETE)
    end
    
    def task(fn_name, args_array, notify_on_complete = false, callback_fn = nil)
      task = JobTask.create(redis,
                            status: States::NEW, 
                            fn: fn_name, 
                            args: args_array, 
                            callback: callback_fn, 
                            notify: notify_on_complete,
                            job: self)
      task.pool = @pool
      
      increment_task_count()
      
      task
    end
    
    def increment_task_count
      @task_count = (task_count || 0) + 1
    end
    
    # add all the tasks to the new_task queue
    def enqueue_tasks
      update!(task_count: task_count)       # finalize the task count
      
      # iterate over all tasks and enqueue each one
      tasks.each do |task|
        task.pool = self.pool
        
        message_id = task.enqueue
        
        # if the response was a numeric job id, then the task was successfully enqueued
        if message_id.is_a? Numeric
          task.status = States::QUEUED
        else
          # try to enqueue the task again
          if task.enqueue.is_a? Numeric
            task.status = States::QUEUED
          else
            task.status = States::NOT_QUEUED
          end
        end
      end
      
      tasks.to_a
    end
    
    # This method blocks until the job is complete
    # If the notify flag is set, it waits for a notification message on the message queue
    # otherwise, it polls redis to see if the job is complete.
    def join(timeout = nil)
      return true if complete?(false)
      
      # this is here because of the side effect that calling complete? has: complete? retrieves the newest value of
      # the job's status field from redis.
      is_complete = complete?
      
      # listen for task completion notification if the task is set up to do that
      if notify && pool
        listen_for_completion_notification(timeout)
      elsif !is_complete    # we're not setup to listen to a queue for task-completion, so poll for job completion if the job isn't marked complete
        wait_for_completion(timeout)
      end
      
      complete?(false)            # return the local (cached) completion status
    end
    
    def listen_for_completion_notification(timeout)
      pool.watch(tube_name)           # subscribe to the task-specific tube
      
      begin
        
        # if the job is already marked complete, just clear any message off the queue
        if complete?(false)
          if pool.peek_ready
            msg = pool.reserve(0)        # poll the queue for a pre-existing message. Timeout immediately if there is no message.
            msg.delete if msg             # delete the message, we don't need it
          end
        else      # the job isn't yet marked as complete, so listen for a "this job is complete" notification message
          msg = pool.reserve(timeout)    # listen for a message; timeout if there is no message within the given timeout period.
          if msg
            msg.delete                    # remove the message from the queue
            
            jobreference = msg.ybody      # the message is a JobReference object
            
            # update the current job's (self's) status attribute with the notification job reference's status
            self.status = jobreference.status
          end
        end
      
      # rescue Beanstalk::TimedOut => e
      #   retry
      ensure
        pool.ignore(tube_name)       # stop watching the queue
      end
    end
    
    def wait_for_completion(timeout)
      if timeout                # we need to poll for a result for timeout seconds
        timeout.to_i.times do
          sleep(1)              # sleep for one second
          break if complete?    # poll for completion, break out if we finally have a result
        end
      else
        loop do
          sleep(1)              # sleep for one second
          break if complete?    # poll for completion, break out if we finally have a result
        end
      end
    end
    
  end
  
  class Client
    def initialize(queues, global_redis_server = {})
      @pool = Beanstalk::Pool.new(queues)
      # @pool.ignore(TUBE_DEFAULT)
      
      @redis = Redis.new(global_redis_server)
    end
    
    def task(fn_name, args_array, notify_on_complete = false, callback_fn = nil)
      task = SimpleTask.create(@redis,
                               status: States::NEW, 
                               fn: fn_name, 
                               args: args_array, 
                               callback: callback_fn, 
                               notify: notify_on_complete)
      task.pool = @pool
      message_id = task.enqueue
      
      # if the response was a numeric job id, then the task was successfully enqueued
      if message_id.is_a? Numeric
        task.status = States::QUEUED
      else
        # try to enqueue the task again
        if task.enqueue.is_a? Numeric
          task.status = States::QUEUED
        else
          task.status = States::NOT_QUEUED
        end
      end
      
      task
    end
    
    def job(notify_on_complete = false, callback_fn = nil)
      j = Job.create(@redis, status: States::NEW, callback: callback_fn, notify: notify_on_complete)
      j.pool = @pool
      j
    end
  end
  
  class Worker
    def initialize(queues, global_redis_server = {}, local_redis_server = {})
      @pool = Beanstalk::Pool.new(queues)
      # @pool.ignore(TUBE_DEFAULT)
      @pool.watch(Tubes::NEW_TASK)
      
      @global_redis_server = Redis.new(global_redis_server)
      @local_redis_server = Redis.new(local_redis_server)
    end
    
    def start
      loop do
        job = @pool.reserve
        
        task = job.ybody
        
        task.pool = @pool
        task.redis = @global_redis_server
        
        # task.reload!([:callback, :notify])
        
        task.update!(status: States::RUNNING)
        
        result = invoke(task)
        
        result_uri = store_result(result)
        
        # update the task's attributes, then enqueue the task's callback (if it exists) and notification (if it exists)
        task.update!(result_uri: result_uri, status: States::COMPLETE)
        task.enqueue_callback
        task.enqueue_notification
        
        # increment the job's completed_task_count
        # if all the job's tasks are complete, enqueue the job's callback (if it exists) and notification (if it exists)
        if task.is_a? JobTask
          j = task.job
          j.pool = @pool
          j.increment_completed_task_count
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