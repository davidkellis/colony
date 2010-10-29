$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'redismodel'
require 'pp'
require 'test/unit'

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
  
  attr_accessor :full_name
end

class TestRedisModel < Test::Unit::TestCase
  def test_redis_model
    r = Redis.new
  
    a = Address.create(r, name: 'David', address1: '123 Main', city: 'San Antonio', state: 'TX', years_at_address: 0)
    a.increment(:years_at_address)
    assert_equal(1, a.years_at_address)
  
    b = Address.load(r, a.id)
    assert_equal('David', b.name)
    assert_equal('123 Main', b.address1)
    assert_nil(b.address2)
    assert_equal('San Antonio', b.city)
    assert_equal('TX', b.state)
    assert_equal('1', b.years_at_address)
    
    b.decrement(:years_at_address)
    assert_equal(0, b.years_at_address)
    
    a.reload!([:years_at_address])
    assert_equal('0', a.years_at_address)
    
    p = Person.create(r, full_name: "David Ellis")
    assert_equal('David Ellis', p.full_name)
  
    c = Address.create(r, name: "Ellis1", person: p, zip: 123)
    c2 = Address.create(r, name: "Ellis2", person: p, zip: 123)
    assert_equal('Ellis1', c.name)
    assert_equal('Ellis2', c2.name)
    assert_equal(p.id, c.person_id)
    assert_equal(p.id, c.person.id)
    assert_equal(p.id, c2.person_id)
    assert_equal(p.id, c2.person.id)
    assert_equal(p.full_name, c.person.full_name)
    assert_equal(2, p.addresses.length)

    c3 = Address.create(r, name: "Ellis3", person: p, zip: 123)
    assert_equal('Ellis3', c3.name)
    assert_equal(p.id, c3.person_id)
    assert_equal(p.id, c3.person.id)
    assert_equal(p.full_name, c3.person.full_name)
    assert_nil(c3.address1)

    assert_equal(3, p.addresses(true).length)
    assert_equal(['Ellis1', 'Ellis2', 'Ellis3'], p.addresses.map{|a| a.name}.sort)
    
    c3.person = nil
    c3.save!
    
    assert_equal(2, p.addresses(true).length)
    assert_nil(c3.person)
    assert_equal(['Ellis1', 'Ellis2'], p.addresses.map{|a| a.name}.sort)
    
    d = Address.load(r, c.id)
    assert_equal(c.id, d.id)
    assert_equal(c.name, d.name)
    assert_equal("Ellis1", d.name)
    assert_equal(p.id, c.person_id)
    assert_equal(p.id, d.person_id)
    assert_equal(p.id, c.person.id)
    assert_equal(p.id, d.person.id)
    assert_equal(c.name, d.name)
  end
end
