require 'rake/gempackagetask'

spec = Gem::Specification.new do |s|
  s.name = "colony"
  s.summary = "A simple distributed task library in Ruby."
  s.description= "A simple distributed task library in Ruby."
  s.requirements = [ 'None' ]
  s.version = "0.0.1"
  s.author = "David Ellis"
  s.email = "davidkellis@gmail.com"
  s.homepage = "http://david.davidandpenelope.com"
  s.platform = Gem::Platform::RUBY
  s.required_ruby_version = '>=1.9'
  s.files = Dir['**/**']
  s.executables = []
  s.test_files = []
  s.has_rdoc = false
end

Rake::GemPackageTask.new(spec).define