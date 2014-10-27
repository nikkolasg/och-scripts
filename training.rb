#!/usr/bin/ruby
# configuration ruby file dsl
#
#
require './config/config.rb'

class Test

    @actions = [:action1]
    
    def initialize(a1,a2=nil,opts = {})
        if opts[:test]
            puts "TEST OPTS"
        else
            puts "TEST INITIALIZE"
        end
    end
    def self.parse action

        Test.send(action.to_sym)

    end
    def self.action1
        puts "ACTION 1 CALLED!"
    end
end

require './ruby_util'
require './get/fetchers'
require './get/getter'
require './cdr'
require './create_tables'
require 'net/ssh'
require './stats/process'
i = 1 
puts App.flow(:MSS).monitors.class

mon = App.flow(:MSS).monitors.first

Tables::create_table_monitors App.flow(:MSS)
