require 'redis'
require 'uuidtools'

def Array.hashify(arr, &blk)
  arr.reduce(Hash.new) do |memo, item|
    memo[item] = blk.call(item)
    memo
  end
end

module RedisModel
  module ClassMethods
    def field(name)
      fields << name
      
      # define accessor methods
      # self.module_eval(<<-METHOD)
      #   def #{name}()
      #     @#{name}
      #   end
      #   
      #   def #{name}=(value)
      #     @#{name} = value
      #   end
      # METHOD
      
      self.module_eval do
        attr_accessor name.to_sym
      end
    end
    
    def fields
      # I was considering this in order to make model inheritance work, but I decided I don't want
      # to both with implementing model inheritance.
      # So, for now, model inheritance is not supported (i.e. one model can't inherit from another
      # with the expectation that the fields from the superclass will transfer to the subclass model).
      # @fields ||= superclass.respond_to?(:fields) ? superclass.fields + [:id] : [:id]
      @fields ||= [:id]
    end
    
    def create(redis, attrs = {})
      m = self.new(attrs)
      m.redis = redis
      m.save!(attrs.keys)
      m
    end
    
    def load(redis, id, attrs = [])
      m = self.new(id: id)
      m.redis = redis
      attrs = fields if attrs.count == 0
      m.reload!(attrs)
      m
    end
  end
  
  def self.included(mod)
    mod.extend(ClassMethods)
    
    mod.module_eval do
      attr_accessor :redis
    end
  end
  
  def initialize(attrs = {})
    do_assignments_to_self(attrs)
  end
  
  # takes a hash and for each key/value pair, does one of the following:
  # 1. If <key>= is a defined method, then the following assignment is made
  #    self.<key> = value
  # 2. Otherwise, the instance variable @<key> is set with the given value
  def do_assignments_to_self(attrs = {})
    attrs.each do |k, v|
      method = "#{k}=".to_sym
      if self.respond_to? method
        self.send(method, v)
      else
        instance_variable_set("@#{k}".to_sym, v)
      end
    end
  end
  
  def increment(att, increment_amt = 1)
    if self.class.fields.include?(att)
      redis.hincrby(key_prefix, att, increment_amt)
    else
      if increment_amt == 1
        redis.incr(key_for(att))
      else
        redis.incrby(key_for(att), increment_amt)
      end
    end
  end
  
  def decrement(att, decrement_amt = 1)
    if self.class.fields.include?(att)
      redis.hincrby(key_prefix, att, -decrement_amt)
    else
      if decrement_amt == 1
        redis.decr(key_for(att))
      else
        redis.decrby(key_for(att), decrement_amt)
      end
    end
  end
  
  def key_prefix
    "#{self.class.name}:#{id}"
  end
  
  def key_for(attribute)
    "#{key_prefix}:#{attribute.to_s}"
  end
  
  def get_local_attribute(attribute)
    if respond_to? attribute
      send(attribute.to_sym)
    else
      instance_variable_get("@#{attribute}".to_sym)
    end
  end
  
  def get_remote_attribute(attribute)
    if self.class.fields.include?(attribute)     # if the attribute is a field, then we want to call redis.hget
      redis.hget(key_prefix, attribute)
    else                                    # otherwise, we want to call redis.get
      redis.get(key_for(attribute))
    end
  end
  
  # Example:
  # get_remote_attributes(:b, :a)
  # => {:b => "0", :a => "23"}
  def get_remote_attributes(attributes = [])
    return {} if attributes.count == 0
    values = redis.hmget(key_prefix, *attributes)
    Hash[attributes.zip(values)]
  end
  
  def set_remote_attribute(attribute, value)
    if self.class.fields.include?(attribute)     # if the attribute is a field, then we want to call redis.hset
      redis.hset(key_prefix, attribute, value)
    else                                    # otherwise, we want to call redis.set
      redis.set(key_for(attribute), value)
    end
  end
  
  def id=(id)
    @id = id
  end
  
  def id
    @id ||= UUIDTools::UUID.random_create.to_s
  end
  
  def reload!(attrs = [])
    # do_assignments_to_self(Array.hashify(attrs) {|attribute| get_remote_attribute(attribute)})
    do_assignments_to_self(get_remote_attributes(attrs))
  end
  
  def reload_all!
    reload!(self.class.fields)
  end
  
  def marshal(attribute)
    method = "marshal_#{attribute}".to_sym
    if respond_to? method
      send(method)
    else
      get_local_attribute(attribute).to_s
    end
  end
  
  def save!(attrs = [])
    attrs.each do |attribute|
      set_remote_attribute(attribute, marshal(attribute))
    end
  end
  
  def save_all!
    save!(self.class.fields)
  end
  
  def update!(attrs = {})
    do_assignments_to_self(attrs)
    save!(attrs.keys)
  end
end