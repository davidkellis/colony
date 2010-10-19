$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'colony'
require 'pp'

module MyFunctions
  def self.sqrt(n)
    Math.sqrt(n)
  end
end

def multiply(*args)
  args.reduce(:*)
end

def delayed_multiply(*args)
  sleep(2)
  args.reduce(:*)
end

def print_hi_on_worker(task_id, result_uri)
  puts "Hi, from the callback for task \"#{task_id}\", who's return value is located at:\n#{result_uri}"
end

def print_job_info(job_id, task_id_result_uri_pairs)
  puts "Hi, from the callback for job \"#{job_id}\", who's return values are located at:\n#{task_id_result_uri_pairs}"
end

def main
  w = Colony::Worker.new(['localhost:11300'], {host: 'localhost'}, {host: 'localhost'})
  w.start
end

main