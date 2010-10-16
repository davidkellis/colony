$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'colony'

def main
  c = Colony::Client.new(['localhost:11300'], {host: 'localhost'})

  puts '************** POLLING **************'

  result1 = c.task(:multiply, [1, 2, 3])
  puts "result1 = c.task(:multiply, [1, 2, 3])"

  begin
    val = result1.value
    puts "result1.value -> #{val}"
  end until val

  result2 = c.task(:multiply, [4, 5, 6])
  puts "result2 = c.task(:multiply, [4, 5, 6])"

  begin
    val = result2.value
    puts "result2.value -> #{val}"
  end until val


  puts '************** BLOCKING **************'

  result3 = c.task(:multiply, [1, 2, 3], true)
  puts "result3 = c.task(:multiply, [1, 2, 3], true)"

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

  # j = c.job
  # rand_numbers = 100.times.map { rand(100) }
  # slices = rand_numbers.each_slice(3)
  # results = slices.map {|slice| j.task(:multiply, slice) }
  # 
  # puts slices.zip(results.map(&:value)).map{|pair| pair.join(" -> ")}.join("\n")
  # 
  # j = c.job
  # j.callback(:join_and_print)
  # results = slices.map {|slice| j.task(:multiply, slice) }

end

main