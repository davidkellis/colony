$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'colony'

def main
  c = Colony::Client.new(['localhost:11300'], {host: 'localhost'})

  puts '************** Polling for completion **************'
  
  result0 = c.task(:multiply, [1, 2, 3])
  puts "result0 = c.task(:multiply, [1, 2, 3])"
  
  begin
    val = result0.value(0)
    puts "result0.value(0) -> #{val}"
  end until val
  
  result1 = c.task(:delayed_multiply, [1, 2, 3])
  puts "result1 = c.task(:delayed_multiply, [1, 2, 3])"
  
  begin
    val = result1.value(1)
    puts "result1.value(1) -> #{val}"
  end until val
  
  result2 = c.task(:multiply, [4, 5, 6])
  puts "result2 = c.task(:multiply, [4, 5, 6])"
  
  begin
    val = result2.value
    puts "result2.value -> #{val}"
  end until val
  
  
  puts '************** Beanstalkd notification of completion **************'
  
  result3 = c.task(:delayed_multiply, [1, 2, 3], true)
  puts "result3 = c.task(:delayed_multiply, [1, 2, 3], true)"
  
  begin
    val = result3.value
    puts "c.task(:multiply, [1, 2, 3], true) -> #{val}"
  end until val
  
  result4 = c.task(:multiply, [4, 5, 6], true)
  puts "result4 = c.task(:multiply, [4, 5, 6], true)"
  
  begin
    val = result4.value
    puts "c.task(:multiply, [4, 5, 6], true) -> #{val}"
  end until val
  
  
  puts '************** Module Method **************'
  
  result5 = c.task("MyFunctions.sqrt", [29], true)
  puts "c.task(\"MyFunctions.sqrt\", [29], true) -> #{result5.value}"
  
  
  puts '************** Callbacks **************'
  
  puts "This task should cause the worker process to print a message with this"
  puts "task's redis id and this task's result (i.e. return value) URI."
  result6 = c.task("MyFunctions.sqrt", [25], true, :print_hi_on_worker)
  puts "c.task(\"MyFunctions.sqrt\", [25], true, :print_hi_on_worker) -> #{result6.value}"

  
  puts '************** Job with subtasks (Polling for job completion) **************'
  
  j = c.job(false, :print_job_info)
  rand_numbers = 100.times.map { rand(100) }      # [99, 57, 61, 75, 39, 35, 20, 8, 34, 91, ...]
  slices = rand_numbers.each_slice(3)             # [[99, 57, 61], [75, 39, 35], [20, 8, 34], [91, 0, 36], ...]
  
  # there should be 34 slices, and therefore 34 tasks
  tasks = slices.map {|slice| j.task(:multiply, slice) }
  
  j.enqueue_tasks
  j.join
  puts slices.zip(tasks.map(&:value)).map {|pair| "#{pair[0].join(' * ')} -> #{pair[1]}"}.join("\n")


  puts '************** Job with subtasks (Beanstalkd notification of job completion) **************'
  
  j = c.job(true, :print_job_info)
  rand_numbers = 100.times.map { rand(100) }      # [99, 57, 61, 75, 39, 35, 20, 8, 34, 91, ...]
  slices = rand_numbers.each_slice(3)             # [[99, 57, 61], [75, 39, 35], [20, 8, 34], [91, 0, 36], ...]
  
  # there should be 34 slices, and therefore 34 tasks
  tasks = slices.map {|slice| j.task(:multiply, slice) }
  
  j.enqueue_tasks
  j.join
  puts slices.zip(tasks.map(&:value)).map {|pair| "#{pair[0].join(' * ')} -> #{pair[1]}"}.join("\n")
end

main