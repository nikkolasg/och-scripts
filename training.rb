#!/usr/bin/ruby
# configuration ruby file dsl
#
#
class SuperTest
    attr_accessor :value
    def initialize value = nil
        @value = value
    end
  def self.custom_class_method
        self.new("CUSTOM VALUE")
  end 
end
class Test < SuperTest 
end
class Test2 < SuperTest
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
puts Test.custom_class_method.delete "youpi"
t.delete ("?") { block_method }
require './ruby_util'
require 'net/ssh'

