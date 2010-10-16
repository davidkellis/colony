$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'colony'
require 'pp'

module MyFunctions
  def self.sqrt(n)
    Math.sqrt(n)
  end
end

def multiply(a, b, c)
  a * b * c
end

def delayed_multiply(a, b, c)
  sleep(2)
  a * b * c
end

def print_hi_on_worker(task_id, result_uri)
  puts "Hi, from the callback for task \"#{task_id}\", who's return value is located at:\n#{result_uri}"
end

def join_and_print(job_id, result_uris)
  puts "Hi, from the callback for job \"#{job_id}\", who's return values are located at:\n#{result_uris}"
end

def main
  w = Colony::Worker.new(['localhost:11300'], {host: 'localhost'}, {host: 'localhost'})
  w.start
end

main