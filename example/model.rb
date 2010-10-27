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
  
  c = Address.create(r, name: "Ellis1", person: p)
  c2 = Address.create(r, name: "Ellis2", person: p)
  pp c
  pp c2
  
  c3 = Address.create(r, name: "Ellis3", person: p)
  c3.person = nil     # TODO: change this so that a save! is required for the relation to be broken. As is, save! isn't required.
  
  d = Address.load(r, c.id)
  pp d  
  puts "p.id == c.person.id => #{p.id == c.person.id}"
  puts "p.id == d.person.id => #{p.id == d.person.id}"
  
  pp p.addresses
end

main