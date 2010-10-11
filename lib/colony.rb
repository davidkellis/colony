require 'mongo'
require 'mongoid'
require 'beanstalk-client'
require 'leiri'

TOPLEVEL = self

module Colony
  # tube names
  DEFAULT_TUBE = "default"
  NEW_TASK_TUBE = "colony_new"
  STATUS_TUBE = "colony_status"
  
  # status states
  STATUS_COMPLETE = "done"
  STATUS_RUNNING = "running"
  STATUS_UNKNOWN = "unknown"
  
  # message types
  MSG_STATUS_QUERY = "status"
  MSG_TASK_RESULT = "result"
  MSG_JOB_INFO = "job"
  
  class Client
    def initialize(queues)
      @pool = Beanstalk::Pool.new(queues)
      # @pool.ignore(DEFAULT_TUBE)
    end
    
    def task(fn_name, args_array, notify_on_completion = nil, callback_fn = nil)
      task = {fn: fn_name, args: args_array, callback: callback_fn, notify: notify_on_completion}
      
      @pool.use(NEW_TASK_TUBE)
      task_id = @pool.yput(task)
      ResultReference.new(task_id, @pool)
    end
    
    # def job(tasks)
    # end
  end
  
  # This class is not thread-safe. Only one ResultReference object per Client object
  # should be used at once. It is safe to call ResultReference#value from different ResultReference objects
  # in different threads *as long as* they were generated from different Client objects.
  class ResultReference
    def initialize(queued_task_id, mq_pool)
      @task_id = queued_task_id
      @pool = mq_pool
      @tube_name = "task#{@task_id}"
    end
    
    def value
      return @result if @result
      
      @pool.watch(@tube_name)
      msg = nil
      
      if @pool.peek_ready
        msg = @pool.reserve(1)        # poll the queue for a pre-existing message. Timeout immediately if there is no message.
      elsif !@query_attempted
        query_task_status           # ask the queen to put our task's result_uri in the queue for us.
        msg = @pool.reserve(1) if @pool.peek_ready  # poll the queue for a message. Timeout immediately if there is no message.
        @query_attempted = true
      end
      
      if msg
        msg.delete                  # remove the message from the queue
        @pool.ignore(@tube_name)    # stop watching the queue
        
        status = msg.ybody
        result = nil
        
        if status[:status] == STATUS_COMPLETE
          result_uri = status[:result_uri]
          uri = LegacyExtendedIRI.new(result_uri)
          db_name, coll_name, result_id = uri.path.split('/').drop(1)

          conn = Mongo::Connection.new(uri.host || "localhost", uri.port || 27017)
          db = conn.db(db_name)
          coll = db.collection(coll_name)
          result = coll.find_one(BSON::ObjectID.from_string(result_id))
          conn.close
        end
        
        @result = result if result
        @result    # returns a BSON::OrderedHash (which descends from Hash)
      end
    end
    
    def query_task_status
      @pool.use(STATUS_TUBE)
      @pool.yput({type: MSG_STATUS_QUERY, task_id: @task_id})
    end
  end
  
  class Worker
    def initialize(queues, db_config_hash)
      @pool = Beanstalk::Pool.new(queues)
      # @pool.ignore(DEFAULT_TUBE)
      @pool.watch(NEW_TASK_TUBE)
      
      Mongoid.configure do |config|
        master = db_config_hash[:master]    # of the form: {host: 'abc', port: 123, db: 'production'}
        slaves = db_config_hash[:slaves]    # each of the form: {host: 'abc', port: 123, db: 'production'}
        config.master = Mongo::Connection.new(master[:host] || "localhost", master[:port] || 27017).db(master[:db])
        if slaves
          config.slaves = slaves.map do |s|
            Mongo::Connection.new(s[:host] || "localhost", s[:port] || 27017, :slave_ok => true).db(s[:db])
          end
        end
        config.persist_in_safe_mode = false
      end
    end
    
    def start
      host = Mongoid.master.connection.host
      port = Mongoid.master.connection.port
      db_name = Mongoid.master.name
      coll_name = ComputedResult.collection_name
      
      loop do
        job = @pool.reserve
        
        task = job.ybody
        callback = task[:callback]
        notify = task[:notify]
        parent_job_id = task[:job_id]
        task_id = job.id
        
        result = invoke(task)
        
        result_id = store_result(task_id, result)
        result_uri = "mongodb://#{host}:#{port}/#{db_name}/#{coll_name}/#{result_id}"
        
        # we let the queen know of all our completed tasks, even if the task has no parent job.
        enqueue_task_complete(parent_job_id, task_id, result_uri)
        
        if callback
          enqueue_task(callback, [task_id, result_uri])
        end
        
        if notify
          notify_task_complete(task_id, result_uri)
        end
        
        job.delete    # mark the job as complete
      end
    end
    
    # send a message to the queen telling her that the task is complete
    # and that its parent job is one step closer to being finished.
    def enqueue_task_complete(job_id, task_id, result_uri)
      status_msg = {type: MSG_STATUS_QUERY, status: STATUS_COMPLETE, job_id: job_id, task_id: task_id, result_uri: result_uri}
      
      @pool.use(STATUS_TUBE)
      @pool.yput(status_msg)
    end
    
    def enqueue_task(fn_name, args_array, notify_on_completion = nil, callback_fn = nil)
      task = {fn: fn_name, args: args_array, callback: callback_fn, notify: notify_on_completion}

      @pool.use(NEW_TASK_TUBE)
      @pool.yput(task)
    end
    
    def notify_task_complete(task_id, result_uri)
      tube_name = "task#{task_id}"
      status_msg = {type: MSG_STATUS_QUERY, status: STATUS_COMPLETE, result_uri: result_uri}
      
      @pool.use(tube_name)
      @pool.yput(status_msg)
    end
    
    def invoke(task)
      fn = task[:fn]
      args = task[:args]
      
      TOPLEVEL.send(fn, *args)
    end
    
    # stores the result in mongodb and returns the document id
    def store_result(task_id, result)
      result = ComputedResult.create(task_id: task_id, value: result)
      result.id
    end
  end
  
  class ComputedResult
    include Mongoid::Document
    
    field :task_id
    field :value
  end
  
  class Queen
    def initialize(queues, db_config_hash)
      @pool = Beanstalk::Pool.new(queues)
      # @pool.ignore(DEFAULT_TUBE)
      @pool.watch(STATUS_TUBE)
      
      Mongoid.configure do |config|
        master = db_config_hash[:master]    # of the form: {host: 'abc', port: 123, db: 'production'}
        slaves = db_config_hash[:slaves]    # each of the form: {host: 'abc', port: 123, db: 'production'}
        config.master = Mongo::Connection.new(master[:host] || "localhost", master[:port] || 27017).db(master[:db])
        if slaves
          config.slaves = slaves.map do |s|
            Mongo::Connection.new(s[:host] || "localhost", s[:port] || 27017, :slave_ok => true).db(s[:db])
          end
        end
        config.persist_in_safe_mode = false
      end
    end
    
    def process_message(msg)
      case msg[:type]
      when MSG_STATUS_QUERY
        process_status_query(msg)
      when MSG_JOB_INFO
        process_job_info(msg)
      when MSG_TASK_RESULT
        process_task_result(msg)
      end
    end
    
    def process_status_query(msg)
      status = msg[:status]
      job_id = msg[:job_id]
      task_id = msg[:task_id]
      result_uri = msg[:result_uri]
      
      if status == STATUS_COMPLETE
        notify_task_complete(task_id, result_uri)
      end
    end
    
    def process_job_info(msg)
      task_count = msg[:task_count]
      callback = msg[:callback]
      notify = msg[:notify]
      job_id = task[:job_id]
      
      j = Job.create(id: job_id, task_count: task_count, callback: callback, notify: notify)
    end
    
    def process_task_result(msg)
      job_id = task[:job_id]
      task_id = task[:task_id]
      result_uri = task[:result_uri]
      
      j = Job.find_one({job_id: job_id})
      t = Task.create(task_id: task_id, result_uri: result_uri)

      if j
        j.tasks << t
        
        # the job is complete if the task_count equals the number of completed tasks
        if j.task_count == j.tasks.count
          if j.callback_fn
            enqueue_task(j.callback_fn, [tasks])
          end
        
          if j.notify_p
            notify_job_complete(job_id, j.tasks)
          end
        end
      end
    end
    
    def start
      loop do
        job = @pool.reserve
        
        msg = job.ybody
        
        process_message(msg)
        
        job.delete    # mark the job as complete
      end
    end
    
    def notify_task_complete(task_id, result_uri)
      tube_name = "task#{task_id}"
      status_msg = {type: MSG_STATUS_QUERY, status: STATUS_COMPLETE, result_uri: result_uri}
      
      @pool.use(tube_name)
      @pool.yput(status_msg)
    end
    
    def notify_job_complete(job_id, tasks)
      tube_name = "job#{job_id}"
      status_msg = {type: MSG_STATUS_QUERY, status: STATUS_COMPLETE, tasks: tasks}
      
      @pool.use(tube_name)
      @pool.yput(status_msg)
    end
    
    def enqueue_task(fn_name, args_array, notify_on_completion = nil, callback_fn = nil)
      task = {fn: fn_name, args: args_array, callback: callback_fn, notify: notify_on_completion}

      @pool.use(NEW_TASK_TUBE)
      @pool.yput(task)
    end
  end
  
  class Task
    include Mongoid::Document
    
    embedded_in :job, :inverse_of => :tasks
    
    field :task_id
    field :result_uri
  end
  
  class Job
    include Mongoid::Document
    
    embeds_many :tasks
    
    field :job_id
    field :task_count
    field :callback_fn
    field :notify_p
  end
end