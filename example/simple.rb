$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'colony'

def main
  c = Colony::Client.new(['localhost:11300'])

  result1 = c.task(:multiply, [1, 2, 3], true)
  result2 = c.task(:multiply, [4, 5, 6], true)

  puts result1.value
  puts result2.value

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