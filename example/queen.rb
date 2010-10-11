$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'colony'

def main
  w = Colony::Queen.new(['localhost:11300'], {master: {host: 'localhost', db: 'colony_test'}})
  w.start
end

main