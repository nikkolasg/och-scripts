#!/usr/bin/ruby
#
# Copyright (C) 2014-2015 Nicolas GAILLY for Orange Communications SA, Switzerland
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# configuration ruby file dsl
#
#
module IncludeModule

    def define_module_function value = nil
        if value
            @@value = value
            puts "Module Function Writer here !"
        else
            puts "Module Function Reader here !"
            return @@value
        end
    end
    module_function :define_module_function
    
     

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
    TEST_CONSTANT = "This is a constant from TEST CLASS"

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
Test.instance_eval do
    var = :va
    var = "@#{var}".to_sym
    define_method :foo do |param = nil|
    if param
        instance_variable_set(var,param)
    else
        puts instance_variable_get(var)
    end
end
end
ttt = Test.new
ttt.foo "foo"
ttt.foo

puts Test::const_get("TEST_CONSTANT")

module ValueModule
    def foo value = nil
        @bar = "bar module value : #{value}" if value
        @bar
    end
end
class T1
    include ValueModule
    attr_accessor :t2
end
class T2
    include ValueModule
end
t1 = T1.new
t2 = T2.new
t1.t2 = t2
t1.foo "t1 value"
t2.foo "t2 value"
puts t1.foo
puts t2.foo

require 'benchmark'
Benchmark.bm do |rep|
    #rep.report("wc -l") { n.times { %x{ wc -l #{f}}.to_i } }
end

require 'optparse'
opts = {}
opt_ = OptionParser.new do |opt|
    opt.on("-s","--scale SCALING","Scaling factor") do |n|
        opts[:scale] = n
    end
end
opt_.parse!
puts "OPTIONS : " + opts.inspect

require_relative 'config'
require_relative 'database/sql'
require_relative 'database/table_util'
fname = "RECORDS_MSS_IN"
db = Database::Mysql.default
db.connect do 
    db.con.query_with_result = false
    db.query("SELECT * from RECORDS_MSS_IN LIMIT 5");
    res = db.con.use_result
    i = 0
    res.each_hash do |row|
        puts "Num of results (#{res.num_rows}) : " + row['calling_number']
        i += 1
        break if i == 10
    end
    res.free
    puts "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    qwr = false
    db.con.query_with_result = qwr
    res =  db.con.query("SELECT * from RECORDS_MSS_IN limit 100");
    res = db.con.use_result unless qwr
    i = 0
    res.each_hash do |row|
        puts "Num of results (#{res.num_rows}) : " + row['calling_number']
        i += 1
        break if i == 10
    end
    res.free
end

def method_without_block 
    print "inside method without block"
end
method_without_block { print "hello in block" }