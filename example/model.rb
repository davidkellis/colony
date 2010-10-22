$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'redismodel'
require 'pp'

class Address
  include RedisModel
  
  # field :id is present by default, no need to declare it manually
  field :name
  field :address1
  field :address2
  field :city
  field :state
  field :zip
  
  field :years_at_address
end

def main
  r = Redis.new
  
  a = Address.create(r, name: 'David', address1: '123 Main', city: 'San Antonio', state: 'TX', years_at_address: 0)
  a.increment(:years_at_address)
  pp a
  
  b = Address.load(r, a.id)
  pp b
end

main