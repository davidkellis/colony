require 'mongo'
require 'mongoid'
require 'beanstalk-client'
require 'leiri'

TOPLEVEL = self

module Colony
  DEFAULT_TUBE = "default"
  NEW_TASK_TUBE = "colony_new"
  STATUS_TUBE = "colony_status"
  
  STATUS_COMPLETE = 'done'
  STATUS_RUNNING = 'running'
  STATUS_UNKNOWN = 'unknown'
  
  MSG_STATUS_QUERY = "status"
  MSG_RESULT_INFO = 'result'
  
  class Client
    def initialize(queues)
      @pool = Beanstalk::Pool.new(queues)
      @pool.ignore(DEFAULT_TUBE)
    end
    
    def task(fn_name, args_array, notify_on_completion = nil, callback_fn = nil)
      task = {fn: fn_name, args: args_array, callback: callback_fn, notify: notify_on_completion}
      
      @pool.use(NEW_TASK_TUBE)
      task_id = @pool.yput(task)
      
      ResultReference.new(task_id, @pool, @server)
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
      msg = @pool.reserve(0)        # poll the queue for a pre-existing message. Timeout immediately if there is no message.

      if msg.nil? && !@query_attempted
        query_task_status
        msg = @pool.reserve(0)      # poll the queue for a message. Timeout immediately if there is no message.
        @query_attempted = true
      end
      
      if msg
        msg.delete                  # remove the message from the queue
        @pool.ignore(@tube_name)    # stop watching the queue
        
        status = msg.ybody
        result = nil
        
        if status[:status] == STATUS_COMPLETE
          uri = LegacyExtendedIRI.new(status[:result_uri])
          db_name, coll_name, result_id = uri.path.split('/').drop(1)

          conn = Mongo::Connection.new(uri.host || "localhost", uri.port || 27017)
          db = conn.db(db_name)
          coll = db.collection(coll_name)
          result = coll.find_one(Mongo::ObjectId.from_string(result_id))
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
      @pool.ignore(DEFAULT_TUBE)
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
        
        result = invoke(task)

        result_id = store_result(job.id, result)
        result_uri = "mongodb://#{host}#{port}/#{db_name}/#{coll_name}/#{result_id}"

        if parent_job
          enqueue_task_complete(parent_job_id, job.id, result_uri)
        end

        if callback
          enqueue_task(callback, [result_uri])
        end

        if notify
          notify_task_status(job.id, result_uri)
        end
        
        job.delete    # mark the job as complete
      end
    end
    
    def enqueue_task_complete(job_id, task_id, result_uri)
      status_msg = {status: STATUS_COMPLETE, result_uri: result_uri}
      
      @pool.use(STATUS_TUBE)
      @pool.yput(status_msg)
    end
    
    def enqueue_task(fn_name, args_array, notify_on_completion = nil, callback_fn = nil)
      task = {fn: fn_name, args: args_array, callback: callback_fn, notify: notify_on_completion}

      @pool.use(NEW_TASK_TUBE)
      @pool.yput(task)
    end
    
    def notify_task_status(task_id, result_uri)
      tube_name = "task#{task_id}"
      status_msg = {status: STATUS_COMPLETE, result_uri: result_uri}
      
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
      result.identify
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
      @pool.ignore(DEFAULT_TUBE)
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
        
        result = invoke(task)

        result_id = store_result(job.id, result)
        result_uri = "mongodb://#{host}#{port}/#{db_name}/#{coll_name}/#{result_id}"

        if parent_job
          enqueue_task_complete(parent_job_id, job.id, result_uri)
        end

        if callback
          enqueue_task(callback, [result_uri])
        end

        if notify
          notify_task_status(job.id, result_uri)
        end
        
        job.delete    # mark the job as complete
      end
    end
  end
  
  class 
  end
end