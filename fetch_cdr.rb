#!/usr/bin/ruby 
#
# Script that list or download 
# the cdr of the specified type.
# INPUT/ OUTPUT in JSON format 
# { host:
#   {   name : hostname
#       switches 
#       [
#          {
#             name: switchname
#             files: [ f1,f2,f3...] 
#          }      
#      ]
#   }
# }
# files are either thoses listed or 
# downloaded depending on operation

require 'optparse'
require './config'
require './logger'
require './fetchers'
require './util'
require './ruby_util'
require 'json'

$opts = {}
$ope = [ :download, :list ]
opt_parse = OptionParser.new do |opt|
    opt.banner = "fetch_cdr.rb OPERATION TYPE OPTIONS"
    opt.separator ""
    opt.separator "TYPE of cdr to operate on (MSS,etc etc)"
    opt.separator "OPERATION: list or download. If download, the JSON format must be passed in stdin. If Download, the files are downloaded into DATA_TMP, then moved into DATA_STORE"
    opt.on("-f","--full","FULL: It will retrieve from every 'hosts' known for this type.") do |t|
        $opts[:full] = true
    end
    opt.on("-h","--host HOST","HOST is where to make the operations.Do NOT pass this AND FULL, otherwise you'll get an error.") do |h|
        $opts[:host] = h
    end
    opt.on("-v","--verbose","Verbose output error") do |v|
        $opts[:v] = v
    end
    opt.on_tail("-h","--help","Help display") do |help|
        puts opt
        exit
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
$type = ARGV[1].upcase

#check if they are coherent 
unless ($ope.include?($operation) && EMMConfig["CDR_TYPES"].include?($type))
    $stderr.puts "Wrong operation / type (#{$operation} / #{$type}" if $opts[:v]
    abort
end
# check options collision
if ($opts[:host] && $opts[:full]) || (!$opts[:host] && !$opts[:full]) 
    $stderr.puts "Specified HOST and TYPE same time or no option at all..." if $opts[:v]
    abort
end

# $hosts contain an array,
# one element if host is specfied or
# many if type is specfied
if $opts[:host] 
    $hosts = [ $opts[:host] ]
else
    h = EMMConfig["#{$type}_HOSTS"]
    $hosts = h.is_a?(String) ? [h] : h
               
end

#check stdin presence
#and parse json
if $operation == :download
    json_in = $stdin.read
    unless json_in && !json_in.empty?
        $stderr.puts "No input paths .!!" if $opts[:v]
        abort
    end
    unless ($json = JSON.parse(json_in,{symbolize_names: true}))
        $stderr.puts "JSON Parsing error. #{$json}" if $opts[:v]
        abort
    end
    $json = RubyUtil::symbolize $json
    if $opts[:host] && $opts[:host] != $json.keys.first.to_s
        $stderr.puts "Host specified in option and host provided do not match !" if $opts[:v]
        abort
    end
# create the json output
elsif $operation == :list
    #create the output json template for this host
    def json_template h
        host = h.to_sym
        json = { host.to_sym => {}}
        json[host] = []
        Util.switches(h).each{|v| json[host] << { name:v, files: [] } }
    json 
    end
    $json = {}
    $hosts.each { |h| $json.merge! json_template h }
end


# return the fetchers that will be used for theses hosts
def get_fetchers
    fetchers = []
    proto = EMMConfig["#{$type}_FETCH_PROTOCOL"].to_sym
    case proto
        when :sftp
            #careful if changing the layout of config file...
            credentials = {}
            credentials[:login] = EMMConfig["#{$type}_#{proto.upcase}_LOGIN"] 
            credentials[:pass] = EMMConfig["#{$type}_#{proto.upcase}_PASS"]
            $hosts.each do |h|
                credentials[:host] = h
                fetchers << Fetchers::FileFetcher.create(proto,credentials)
            end
    end
    fetchers
end

def perform fetcher
    base_path = EMMConfig["#{$type}_BASE_DIR"] + "/"
    fetcher.connect do
        $json[fetcher.host.to_sym].each do |sw_h|
            path = base_path + sw_h[:name] + "/"

            if $operation == :list
                sw_h[:files] = sw_h[:files] + fetcher.list_files_from(path).to_a 

            elsif $operation == :download
                local_path = Util.data_path(EMMConfig["DATA_DOWN_DIR"],sw_h[:name])
                # download them
                fetcher.download_files local_path,path,sw_h[:files]
                ## move them
                store_path  = Util.data_path(EMMConfig["DATA_STORE_DIR"],sw_h[:name])
                cmd = "mv -t #{store_path} #{sw_h[:files].map { |f| local_path + "/" + f }.join(' ')} "
                if !system(cmd)
                    Logger.<<($0,"ERROR","Error while mv command #{cmd[0..40]}")
                    $stderr.puts "Error while mv cmd #{cmd[0..40]}" if $opts[:v]
                   abort
                end 
            end
        end
    end
end
begin
    fetchers = get_fetchers
    fetchers.each { |f| perform f }
    if $operation == :list
        $stdout.puts JSON.generate($json)
    end
rescue => e
    $stderr.puts e.message
    abort
end
exit # safe exit
