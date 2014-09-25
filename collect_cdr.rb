#!/usr/bin/ruby
#
# collect cdr according to the type

require 'optparse'
require './logger'
require './datalayer'

$opts = {}

opt_parse = OptionParse.new do |opt|
    opt.banner = "collect_cdr.rb TYPE [OPTIONS]"
    opt.separator ""
    opt.on("-v","--verbose","Verbose output") do |v|
        $opts[:v] = true
    end
end


