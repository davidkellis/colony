class ModuleTemplate
  def initialize(template)
    @template = template
  end
  
  def fill_in_template(parameters)
    @template.gsub(/\!{([a-zA-Z][a-zA-Z0-9_]+)\}/) {|match| parameters[$1.to_sym] }
  end
  
  def generate(parameters = {})
    mod = Module.new
    mod.module_eval(fill_in_template(parameters))
    mod
  end
end