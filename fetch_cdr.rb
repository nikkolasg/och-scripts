#!/usr/bin/ruby 
#
# Script that list or download 
# the cdr of the specified type.
# OUTPUT in JSON format for LIST
# { host:
#   {   name : hostname
#       switch 
#       {
#           name: switchname
#           files: [ f1,f2,f3...] 
#       }
#   }
# }
# OUTPUT for DOWNLOAD (no json)
# f1
# f2
# f3
# ...

require 'optparse'
require './config'
require './logger'
require './fetchers'

$opts = {}
$ope = [ :download, :list ]
opt_parse = OptionParser.new do |opt|
    opt.banner = "fetch_cdr.rb TYPE OPERATION OPTIONS"
    opt.separator ""
    opt.separator "TYPE of cdr to operate on (MSS,etc etc)"
    opt.separator "OPERATION: list or download. The path must be passed into stdin"
    opt.on("-f","--full","FULL: It will retrieve from every 'hosts' known for this type.") do |t|
        $opts[:full] = true
    end
    opt.on("-h","--host HOST","HOST is where to make the operations.Do NOT pass this AND FULL, otherwise you'll get an error.") do |h|
        $opts[:host] = h
    end
    opt.on("-v","--verbose","Verbose output") do |v|
        $opts[:v] = v
    end
end

opt_parse.parse!
#check presence of cmd line arg
unless ARGV[0] && ARGV[1]
    $stderr.puts "Wrong commands..." if $opts[:v]
    $stderr.puts opt_parse.to_s if $opts[:v]
    abort
end

$operation = ARGV[0].downcase.to_sym
$type = ARGV[0].upcase.to_sym

#check if they are coherent 
unless $ope.include? $operation && EMMConfig["CDR_TYPES"].include? $type
    $stderr.puts "Wrong operation / type (#{$operation} / #{$type}" if $opts[:v]
    abort
end

# check options collision
if ($opts[:host] && $opts[:full])
    $stderr.puts "Specified HOST and TYPE same time..." if $opts[:v]
    abort
end

# $hosts contain an array,
# one element if host is specfied or
# many if type is specfied
if $opts[:host] 
    $hosts = [ $opts[:host] ]
else
    $hosts = EMMConfig["#{$opts[:type]}_SWITCHES"]
end

def get_fetchers
   fetchers = []
   proto = EMMConfig["#{$type}_FETCH_PROTOCOL"].to_sym
   case proto
       where :local
            #do local
       where :sftp
            credentials = {}
            credentials[:login] = EMMConfig["#{$type}_#{proto.upcase}_LOGIN"] 
            credentials[:pass] = EMMConfig["#{$type}_#{proto.upcase}_PASS"]
            $hosts.each do |h|
                credentials[:host] = h
                fetchers << Fetchers::FileFetcher.create(proto,credentials)
            end

   end
end
