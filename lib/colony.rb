require 'pp'
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

    def initialize_id
      @id ||= UUIDTools::UUID.random_create.to_s
    end
    
    def enqueue(tube)
      @pool.use(tube)
      @pool.yput(self)
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
        attribute :fn
        attribute :args
        attribute :callback
        attribute :result_uri
        attribute :notify
        attribute :status
      end
    end
    
    def enqueue(tube = Tubes::NEW_TASK)
      super(tube)
    end
    
    def enqueue_callback(tube = Tubes::NEW_TASK)
      if callback
        callback_task = SimpleTask.create(fn: self.callback, args: [self.full_id, self.result_uri])
        callback_task.pool = @pool
        message_id = callback_task.enqueue(tube)
      end
    end
    
    def enqueue_notification
      if notify
        @pool.use(self.tube_name)
        @pool.yput(self)
      end
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
    include Message
    include TaskMessage
    
    def initialize(attrs = {})
      super(attrs.merge(type: Message::SIMPLE_TASK))
    end
  end
  
  class JobReference
    attr_accessor :id, :status
    
    def initialize(job)
      self.id = job.id
      self.status = job.status
    end
  end
  
  class Job < Ohm::Model
    include Message
    
    attribute :status
    attribute :callback
    attribute :notify
    attribute :task_count
    
    counter :completed_task_count
    # collection :tasks, JobTask            # this might be the wrong way of doing this, as I don't think it's memoized.
    
    def initialize(attrs = {})
      super(attrs.merge(type: Message::JOB))
    end
    
    def tasks(reload = false)
      if reload
        @tasks = find_tasks.to_a
      else
        @tasks ||= find_tasks.to_a
      end
    end
    
    def find_tasks
      JobTask.find(:job_id => self.id)
    end
    
    # do the job-completion tasks if the job's subtasks are all complete
    def increment_completed_task_count
      new_completed_count = self.incr(:completed_task_count)
      
      # the job is complete if the completed_task_count == the total task count
      if new_completed_count == task_count
        mark_complete!
        enqueue_callback
        enqueue_notification
      end
    end
    
    def mark_complete!
      update(status: States::COMPLETE)      # mark the job as complete
    end
    
    # call when all the job's tasks are complete
    def enqueue_callback(tube = Tubes::NEW_TASK)
      if callback
        callback_task = SimpleTask.create(fn: callback, args: [full_id, task_id_and_result_uri_pairs])
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
        @pool.use(self.tube_name)
        @pool.yput(notification)
      end
    end
    
    def complete?(reload = true)
      reload!(:status) if reload
      (status == States::COMPLETE)
    end
    
    def task(fn_name, args_array, notify_on_complete = false, callback_fn = nil)
      task = JobTask.create(status: States::NEW, 
                            fn: fn_name, 
                            args: args_array, 
                            callback: callback_fn, 
                            notify: notify_on_complete,
                            job: self)
      tasks << task
      task
    end
    
    # add all the tasks to the new_task queue
    def enqueue_tasks
      update(task_count: tasks.size)       # finalize the task count
      
      # iterate over all tasks and enqueue each one
      tasks.each do |task|
        task.pool = self.pool
        
        # puts 'enqueuing'
        pp task
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
      
      is_complete = complete?
      
      # listen for task completion notification if the task is set up to do that
      if notify && @pool
        @pool.watch(tube_name)           # subscribe to the task-specific tube
        
        begin
          
          # if the job is already marked complete, just clear any message off the queue
          if is_complete
            if @pool.peek_ready
              msg = @pool.reserve(0)        # poll the queue for a pre-existing message. Timeout immediately if there is no message.
              msg.delete if msg             # delete the message, we don't need it
            end
          else      # the job isn't yet marked as complete, so listen for a "this job is complete" notification message
            msg = @pool.reserve(timeout)    # listen for a message; timeout if there is no message within the given timeout period.
            if msg
              msg.delete                    # remove the message from the queue
              
              jobreference = msg.ybody      # the message is a JobReference object
              
              # update the current job's (self's) status attribute with the notification job's status
              self.status = jobreference.status
            end
          end
        
        # rescue Beanstalk::TimedOut => e
        #   retry
        ensure
          @pool.ignore(tube_name)       # stop watching the queue
        end
      elsif !is_complete    # we're not setup to listen to a queue for task-completion, so poll for job completion if the job isn't marked complete
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
      
      complete?(false)            # return the local (cached) completion status
    end
  end
  
  class JobTask < Ohm::Model
    include Message
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
        
        pp task
        
        result = invoke(task)
        
        result_uri = store_result(result)
        
        # update the task's attributes
        task.update(result_uri: result_uri, status: States::COMPLETE)
        if task.is_a? JobTask
          j = task.job
          j.pool = @pool
          j.increment_completed_task_count
        end
        
        task.enqueue_callback
        task.enqueue_notification
        
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