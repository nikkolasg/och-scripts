#!/usr/bin/ruby
# configuration ruby file dsl
#
#
module IncludeModule
    attr_accessor :module_value
    def mod
        puts "Module Method Mixin"
    end
    class ModuleClass
        include IncludeModule
    end 
end

class SuperTest
    @@subclass = {}
    def self.subclass
        @@subclass
    end
    def initialize value = nil
        self.send(:value=,value)
    end
    def value=(v)
        puts "INSIDE VALUE FUNCTION"
        @value = v
    end
    def value
        @value
    end
    def self.custom_class_method value
        self.new(value)
    end 
    def self.register_subclass type
        @@subclass[type] = self
        puts "INSIDE REGISTER ! #{self} => #{type}"
    end
end
class Test < SuperTest 
    self.register_subclass "Test Class"
    class << self
        include IncludeModule
    end

end
class Test2 < SuperTest
    self.register_subclass "Test 2 Class !"
    include IncludeModule

    def module_value
        18
    end
end
def test_action(klass, action)

    klass.instance_eval do 
        define_method action do |*args, &block|
            puts "#{klass.name}(v = #{@value}) : #{action} => #{args.inspect}"
            puts "BLOCK RESULT => #{instance_eval(&block)}" if block 
        end

        define_method :block_method do
            "INSIDE CUSTOM DEFINED BLOCK METHOD"
        end
    end
end
test_action(Test,:delete)
test_action(Test2,:reset)
t = Test.new "test"
t2 = Test2.new "test2"
t.delete "1","2"
t2.reset "1","2"
t.delete ("?") { block_method }
Test.mod
t3 = Test.custom_class_method "custom value in initoliaze"
puts t3.value
cn = :ModuleClass
puts IncludeModule::const_get(cn).new.class.name
name = :value
attr_name = "@#{name}"

print "BEFORE : #{t3.value} ==> "
t3.instance_variable_set(attr_name,56)
print "AFTER : #{t3.value}.\n"

def custom_method arr
    arr.map! { |v| v+1 }
    puts "Hello custom method"
end

require './ruby_util'

arr = (0..55).to_a
RubyUtil::partition arr do |sub|
    puts "Partition size #{sub.size} : #{sub.inspect}"
end
