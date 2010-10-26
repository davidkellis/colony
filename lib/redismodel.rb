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
    
    # This method will read a redis set with a key-name of "<this model class name>:id:relation".
    # The members of the set are the ids of the model objects that belong to this model object, on the specified has_many relation.
    # Example:
    #   has_many :tasks, JobTask
    def has_many(relation_name, other_model_class_name)
      
      has_many_relations << relation_name
      
      # define finder method and getter method
      self.module_eval(<<-METHOD)
        def find_#{relation_name}()
          ids = ids_for_has_many_relation(#{relation_name})
          ids.map {|id| #{other_model_class_name}.load(redis, id) }
        end
        
        def #{relation_name}(reload = false)
          if reload
            @#{relation_name} = find_#{relation_name}()
          else
            @#{relation_name} ||= find_#{relation_name}()
          end
        end
      METHOD
    end
    
    def has_many_relations
      @has_many_relations ||= []
    end
    
    # Example:
    #   belongs_to :job, Job, :tasks
    def belongs_to(relation_name, other_model_class_name, reverse_relation)
      
      belongs_to_relations << [relation_name, other_model_class_name, reverse_relation]
      
      # define field for the <relation>_id
      # define getter/setter for related model object
      self.module_eval(<<-METHOD)
        field :#{relation_name}_id        # creates ..._id getter/setter and registers ..._id as a field
        
        def #{relation_name}
          return @#{relation_name} if @#{relation_name} && @#{relation_name}.id == #{relation_name}_id
          @#{relation_name} = #{other_model_class_name}.load(redis, #{relation_name}_id)
        end
        
        def #{relation_name}=(model_obj)
          unregister_from_has_many_relation_on("#{other_model_class_name}", self.#{relation_name}_id, "#{reverse_relation}")
          
          @#{relation_name} = model_obj
          self.#{relation_name}_id = model_obj.id
          save!([:#{relation_name}_id])
          @#{relation_name}
        end
      METHOD
    end
    
    def belongs_to_relations
      @belongs_to_relations ||= []
    end
    
    def create(redis, attrs = {})
      m = self.new(attrs) { |obj| obj.redis = redis }
      keys_to_save = attrs.keys
      # if the id attribute isn't passed in on the attrs hash, then we want to add it to the list of attributes to save to redis
      keys_to_save << :id unless attrs.keys.include?(:id)
      m.save!(keys_to_save)
      m
    end
    
    def load(redis, id, attrs = [])
      m = self.new(id: id) { |obj| obj.redis = redis }
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
    yield(self) if block_given?
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
  
  def key_for_has_many_relation_on_other_model(model_class_name, model_object_id, reverse_relation_name)
    "#{model_class_name}:#{model_object_id}:#{reverse_relation_name}"
  end
  
  # returns a key (string) of the form: <this model class name>:id:relation
  def key_for_has_many_relation(relation)
    "#{key_prefix}:#{relation}"
  end
  
  # returns an array consisting of the ids of the model objects that belong_to this model object
  def ids_for_has_many_relation(relation)
    redis.smembers(key_for_has_many_relation(relation))
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
    if self.class.fields.include?(attribute)      # if the attribute is a field, then we want to call redis.hset
      redis.hset(key_prefix, attribute, value)
    else                                          # otherwise, we want to call redis.set
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
    do_assignments_to_self(unmarshal_attributes_hash(get_remote_attributes(attrs)))
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
  
  def unmarshal(attribute, value)
    method = "unmarshal_#{attribute}".to_sym
    if respond_to? method
      send(method)
    else
      value
    end
  end
  
  def unmarshal_attributes_hash(hash)
    # perform any necessary unmarshalling
    hash.merge(hash) do |attribute, value|
      unmarshal(attribute, value)
    end
  end
  
  # add this model object's id to the redis set
  # with a key-name of "<relation model class name>:id:relation"
  def register_in_has_many_relation_on(local_relation_name, other_model_class_name, other_model_has_many_relation)
    # redis.sadd(other_model_object.key_for_has_many_relation(other_model_has_many_relation), self.id)
    model_id = self.send("#{local_relation_name}_id".to_sym)
    if(model_id)
      redis.sadd(key_for_has_many_relation_on_other_model(other_model_class_name, model_id, other_model_has_many_relation), self.id)
    end
  end

  def unregister_from_has_many_relation_on(other_model_class_name, model_id, other_model_has_many_relation)
    if(model_id)
      redis.srem(key_for_has_many_relation_on_other_model(other_model_class_name, model_id, other_model_has_many_relation), self.id)
    end
  end
  
  def save!(attrs = [])
    attrs.each do |attribute|
      set_remote_attribute(attribute, marshal(attribute))
    end
    
    # iterate over all belongs_to relations and add this model object's id to the redis set
    # with a key-name of "<relation model class name>:id:relation"
    self.class.belongs_to_relations.each do |relation_3_tuple|
      # each tuple is o the form: [relation_name, other_model_class_name, reverse_relation]
      local_relation_name = relation_3_tuple[0]
      reverse_relation_class_name = relation_3_tuple[1]   # second item in the tuple is the class name of the model which has the has_many relation with this model class
      reverse_relation_name = relation_3_tuple[2]         # the third item in the tuple is the relation name on the other model
      
      register_in_has_many_relation_on(local_relation_name, reverse_relation_class_name, reverse_relation_name)
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