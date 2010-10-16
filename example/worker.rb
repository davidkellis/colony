$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'colony'

def multiply(a, b, c)
  a * b * c
end

def main
  # w = Colony::Worker.new(['localhost:11300'], {master: {host: 'localhost', db: 'colony_test'}})
  w = Colony::Worker.new(['localhost:11300'], {host: 'localhost'}, {host: 'localhost'})
  w.start
end

main