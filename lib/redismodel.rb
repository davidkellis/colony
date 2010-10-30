require 'set'
require 'redis'
require 'uuidtools'
require 'moduletemplate'

# Array.hashify([2, 3, 4]) {|i| 2*i }
# -> {2 => 4, 3 => 6, 4 => 8}
def Array.hashify(arr, &blk)
  arr.reduce(Hash.new) do |memo, item|
    memo[item] = blk.call(item)
    memo
  end
end

module RedisModel
  module ModuleTemplates
    # params: field_name
    FieldGetterAndSetter = ModuleTemplate.new(<<-'MOD')
      def !{field_name}()
        @!{field_name}
      end
      
      def !{field_name}=(value)
        changed_fields << :!{field_name}
        @!{field_name} = value
      end
    MOD
    
    # params: relation_name, other_model_class_name
    # define finder method and getter method
    HasManyFinderAndGetter = ModuleTemplate.new(<<-'MOD')
      def find_!{relation_name}()
        ids = ids_for_has_many_relation("!{relation_name}")
        ids.map {|id| !{other_model_class_name}.load(redis, id) }
      end
      
      def !{relation_name}(reload = false)
        if reload
          @!{relation_name} = find_!{relation_name}()
        else
          @!{relation_name} ||= find_!{relation_name}()
        end
      end
    MOD
    
    # params: relation_name, other_model_class_name, reverse_relation
    # define field for the <relation>_id
    # define getter/setter for related model object
    BelongsToGetterAndSetter = ModuleTemplate.new(<<-'MOD')
      def self.included(mod)
        mod.module_eval do
          field :!{relation_name}_id        # creates ..._id getter/setter and registers ..._id as a field
        end
      end
      
      # NOTE: this is a hack to get the class object to which the given other_model_class_name parameter points.
      # def !{other_model_class_name}_class()
      #   @!{other_model_class_name}_class ||= !{other_model_class_name}
      # end
      
      def !{relation_name}_id_last_saved()
        @!{relation_name}_id_last_saved
      end
      
      def !{relation_name}_id_last_saved=(value)
        @!{relation_name}_id_last_saved = value
      end
      
      def !{relation_name}(reload = false)
        return @!{relation_name} if !reload && @!{relation_name} && @!{relation_name}.id == !{relation_name}_id
        @!{relation_name} = !{relation_name}_id ? !{other_model_class_name}.load(redis, !{relation_name}_id) : nil
      end
      
      def !{relation_name}=(model_obj)
        @!{relation_name} = model_obj
        self.!{relation_name}_id = model_obj ? model_obj.id : nil
        @!{relation_name}
      end
    MOD
  end
  
  module ClassMethods
    def field(name)
      fields << name
      
      self.module_eval do
        include ModuleTemplates::FieldGetterAndSetter.generate(field_name: name.to_s)
      end
    end
    
    def fields
      # I was considering this in order to make model inheritance work, but I decided I don't want
      # to both with implementing model inheritance.
      # So, for now, model inheritance is not supported (i.e. one model can't inherit from another
      # with the expectation that the fields from the superclass will transfer to the subclass model).
      # @fields ||= superclass.respond_to?(:fields) ? superclass.fields + [:id] : [:id]
      @fields ||= Set.new([:id])
    end
    
    # This method will read a redis set with a key-name of "<this model class name>:id:relation".
    # The members of the set are the ids of the model objects that belong to this model object, on the specified has_many relation.
    # Example:
    #   has_many :tasks, 'JobTask'
    def has_many(relation_name, other_model_class_name)
      
      has_many_relations << relation_name
      
      include ModuleTemplates::HasManyFinderAndGetter.generate(relation_name: relation_name,
                                                               other_model_class_name: other_model_class_name)
    end
    
    def has_many_relations
      @has_many_relations ||= Set.new
    end
    
    # Example:
    #   belongs_to :job, 'Job', :tasks
    def belongs_to(relation_name, other_model_class_name, reverse_relation)
      
      belongs_to_relations << [relation_name, other_model_class_name, reverse_relation]
      
      include ModuleTemplates::BelongsToGetterAndSetter.generate(relation_name: relation_name,
                                                                 other_model_class_name: other_model_class_name,
                                                                 reverse_relation: reverse_relation)
    end
    
    def belongs_to_relations
      @belongs_to_relations ||= Set.new
    end
    
    def belongs_to_relation_names
      if @last_belongs_to_relations == belongs_to_relations
        @cached_relation_names
      else
        @last_belongs_to_relations = belongs_to_relations
        @cached_relation_names = @last_belongs_to_relations.map {|tuple| tuple[0] }.to_set
      end
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
  
  # This method returns the Class object to which the class name, classname, refers.
  # def resolve_classname_to_class(classname)
  #   self.send("#{classname}_class".to_sym)
  # end
  
  def set_field(field_name, value)
    instance_variable_set("@#{field_name}".to_sym, value)
  end
  
  def changed_fields()
    @changed_fields ||= Set.new
  end
  
  # Use this method to unflag a field or set of fields as "changed".
  # The set of changed_fields is used by the save! method.
  # This will remove all the fields from the changed_fields set if fields = nil
  # otherwise, only the fields specified in the fields array/set will be unflagged as 'changed'.
  def unflag_changed_fields(fields = nil)
    if fields
      @changed_fields = changed_fields - fields.to_set
    else
      @changed_fields = Set.new
    end
  end

  # takes a hash and for each key/value pair, does one of the following:
  # 1. If <key>= is a defined method, then the following assignment is made
  #    self.<key> = value
  # 2. Otherwise, the instance variable @<key> is set with the given value
  def do_assignments_to_self(attrs = {})
    attrs.each do |k, v|
      method = "#{k}=".to_sym
      if !self.class.fields.include?(k) && self.respond_to?(method)
        self.send(method, v)
      else
        instance_variable_set("@#{k}".to_sym, v)
      end
    end
  end
  
  # this method increments the value of the given attribute on both the server side and client side,
  # and returns the resulting counter total
  # No save! is required to persist the counter value to redis.
  def increment(att, increment_amt = 1)
    new_value = if self.class.fields.include?(att)
                  redis.hincrby(key_prefix, att, increment_amt)
                else
                  if increment_amt == 1
                    redis.incr(key_for(att))
                  else
                    redis.incrby(key_for(att), increment_amt)
                  end
                end
    do_assignments_to_self(att => new_value)
    new_value
  end
  
  # this method decrements the value of the given attribute on both the server side and client side,
  # and returns the resulting counter total
  # No save! is required to persist the counter value to redis.
  def decrement(att, decrement_amt = 1)
    new_value = if self.class.fields.include?(att)
                  redis.hincrby(key_prefix, att, -decrement_amt)
                else
                  if decrement_amt == 1
                    redis.decr(key_for(att))
                  else
                    redis.decrby(key_for(att), decrement_amt)
                  end
                end
    do_assignments_to_self(att => new_value)
    new_value
  end
  
  def key_prefix
    "#{self.class.name}:#{id}"
  end
  
  def key_for(attribute)
    "#{key_prefix}:#{attribute.to_s}"
  end
  
  def key_for_has_many_relation_on_other_model(model_class_name, model_object_id, reverse_relation_name)
    # resolved_model_class = resolve_classname_to_class(model_class_name)
    # "#{resolved_model_class.name}:#{model_object_id}:#{reverse_relation_name}"
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
    attributes = attributes.to_a
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
    do_assignments_to_self(unmarshal_attributes_hash(get_remote_attributes(attrs)))
    
    reload_last_saved_ids_for_belongs_to_relations(attrs)
  end
  
  def reload_all!
    reload!(self.class.fields)
  end
  
  # Takes a list of field names. If any of the field names represent a field id used in a belongs_to relation
  # then the current value of the id field (e.g. address_id) is used to represent the last-saved (i.e. last known)
  # id of the model object to which this model object belongs.
  def reload_last_saved_ids_for_belongs_to_relations(field_names)
    # figure out which of the field_names are belongs_to relation model ids
    reloaded_belongs_to_relation_ids = field_names.to_set & self.class.belongs_to_relation_names.map{|name_sym| "#{name_sym}_id".to_sym }.to_set
    # for each relation model id field, update the last saved model object id with the currently referenced relation model id
    reloaded_belongs_to_relation_ids.each do |field_id_sym|
      update_last_saved_belongs_to_model_id(field_id_sym)
    end
  end
  
  # relation_name_id_field is as symbl of the form:  :<relation name>_id
  def update_last_saved_belongs_to_model_id(relation_name_id_field, current_id = nil)
    current_id ||= self.send(relation_name_id_field)
    self.send("#{relation_name_id_field}_last_saved=", current_id)
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
      send(method, value)
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
    local_relation_name_sym = "#{local_relation_name}_id".to_sym
    current_related_model_id = self.send(local_relation_name_sym)
    last_saved_model_id = self.send("#{local_relation_name}_id_last_saved".to_sym)
    if(current_related_model_id && last_saved_model_id != current_related_model_id)
      redis.sadd(key_for_has_many_relation_on_other_model(other_model_class_name, current_related_model_id, other_model_has_many_relation), self.id)
      update_last_saved_belongs_to_model_id(local_relation_name_sym, current_related_model_id)
    end
  end
  
  # remove this model object's id to the redis set
  # with a key-name of "<relation model class name>:id:relation"
  def unregister_from_has_many_relation_on(local_relation_name, other_model_class_name, other_model_has_many_relation)
    current_related_model_id = self.send("#{local_relation_name}_id".to_sym)
    last_saved_model_id = self.send("#{local_relation_name}_id_last_saved".to_sym)
    if(last_saved_model_id && last_saved_model_id != current_related_model_id)
      redis.srem(key_for_has_many_relation_on_other_model(other_model_class_name, last_saved_model_id, other_model_has_many_relation), self.id)
    end
  end
  
  # takes a set of field names and determines if any of them are belongs_to relation names
  # of the field names that are belongs_to relation_names, it converts the field names to
  # the corresponding id field names
  # For example, if :address and :account are belongs_to relation names, then
  # identify_and_convert_id_field_names(Set.new([:name, :address, :account, :age]))
  # -> #<Set: {:address_id, :account_id}>
  # Returns a set.
  def identify_and_convert_id_field_names(field_names)
    # identify which field_names are also belongs_to relation names
    field_names = (self.class.belongs_to_relation_names & field_names)
    field_names.map {|field_name| "#{field_name}_id".to_sym }.to_set
  end
  
  # Accepts an array of field names
  def save!(attrs = nil)
    attrs ||= changed_fields
    
    attrs = attrs.to_set
    
    attrs = attrs + identify_and_convert_id_field_names(attrs)
    
    # save the new field values in Redis
    # we only want to call set_remote_attribute for attributes that were defined with the 'field' method/"macro".
    (attrs & self.class.fields).each do |attribute|
      set_remote_attribute(attribute, marshal(attribute))
    end
    
    # now, for each field we saved, remove the flag that says that the field has been "changed"
    # in other words, reset the "changed" flag on the fields we just saved.
    unflag_changed_fields(attrs)
    
    # Now, tear down any relationships with model objects that this object is no longer related to,
    #   and record relationships with the model objects that this object is now related to.
    # In other words, remove old relationships, add new relationships.
    # We accomplish this by doing the following:
    #   Iterate over all belongs_to relations and add this model object's id to the redis set
    #   with a key-name of "<relation model class name>:id:relation"
    self.class.belongs_to_relations.each do |relation_3_tuple|
      # each tuple is o the form: [relation_name, other_model_class_name, reverse_relation]
      local_relation_name = relation_3_tuple[0]
      other_model_class_name = relation_3_tuple[1]   # second item in the tuple is the class name of the model which has the has_many relation with this model class
      reverse_relation = relation_3_tuple[2]         # the third item in the tuple is the relation name on the other model
      
      local_relation_name_sym = "#{local_relation_name}_id".to_sym
      
      # if attrs.include?(local_relation_name_sym)
        unregister_from_has_many_relation_on(local_relation_name, other_model_class_name, reverse_relation)
        register_in_has_many_relation_on(local_relation_name, other_model_class_name, reverse_relation)
      # end
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