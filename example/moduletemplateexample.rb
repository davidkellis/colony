$: << File.join(File.expand_path(File.dirname(__FILE__)), "..", "lib")

require 'moduletemplate'

GettersAndSetters = ModuleTemplate.new(<<-'MOD')
  def !{property_name}()
    @!{property_name}
  end
  
  def !{property_name}=(value)
    @!{property_name} = value
  end
MOD

QuotedGetterSetter = ModuleTemplate.new(<<-'MOD')
  def !{property_name}()
    @!{property_name}
  end
  
  def !{property_name}=(value)
    @!{property_name} = "\"#{value}\""
  end
MOD

IdGetterSetter = GettersAndSetters.generate(property_name: "id")

QuotedNameGetterSetter = QuotedGetterSetter.generate(property_name: "name")

class Test
  include IdGetterSetter
  include QuotedNameGetterSetter
end

t = Test.new
puts "t.id.nil? -> #{t.id.nil?}"
t.id = 5
puts "t.id = 5"
puts "t.id -> #{t.id}"

t.name = "David"
puts t.name