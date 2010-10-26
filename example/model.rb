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
  
  belongs_to :person, 'Person', :addresses
end

class Person
  include RedisModel
  
  has_many :addresses, 'Address'
end

def main
  r = Redis.new
  
  a = Address.create(r, name: 'David', address1: '123 Main', city: 'San Antonio', state: 'TX', years_at_address: 0)
  a.increment(:years_at_address)
  pp a
  
  b = Address.load(r, a.id)
  pp b
  
  p = Person.create(r, name: "David")
  pp p
  
  c = Address.create(r, name: "David Ellis", person: p)
  pp c
  
  d = Address.load(r, c.id)
  pp d
  
  puts "p.id == c.person.id => #{p.id == c.person.id}"
  puts "p.id == d.person.id => #{p.id == d.person.id}"
end

main