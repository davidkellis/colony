$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'colony'

def main
  c = Colony::Client.new(['localhost:11300'], {host: 'localhost'})

  puts '************** POLLING **************'

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


  puts '************** BLOCKING **************'

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
  puts "c.task(\"MyFunctions.sqrt\", [25], true, :print_hi_on_worker) -> #{result5.value}"

  
  # puts '************** Job with 3 subtasks **************'
  # 
  # j = c.job(callback: :join_and_print)
  # rand_numbers = 100.times.map { rand(100) }      # [99, 57, 61, 75, 39, 35, 20, 8, 34, 91, ...]
  # slices = rand_numbers.each_slice(3)             # [[99, 57, 61], [75, 39, 35], [20, 8, 34], [91, 0, 36], ...]
  # results = slices.map {|slice| j.task(:multiply, slice) }
  # 
  # puts slices.zip(results.map(&:value)).map{|pair| pair.join(" -> ")}.join("\n")
end

main