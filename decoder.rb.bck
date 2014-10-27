#!/usr/bin/ruby

## Decode a file or all files in a directory
## according to specified format
## output in a JSON format
# filename :
# { 
#   timestamp: ----
#   values:  {
#       MOC:456
#       FOW:1004
#       ...
#   }}
## Can save the dump file if specified

require 'optparse'
require 'json'
require './config'
require './util'

$opts = {}

opt_parser = OptionParser.new do |opt|
	opt.banner = "Usage: decoder.rb -t TYPE NAME [NAME2 ...]"
	opt.separator ""
	opt.separator "-> NAME : can be a file name or a directory.In case ofa directory, every files in the directory will be decoded using the specified format. "
	opt.separator ""
	opt.on("-t","--type TYPE","Type of file you want to decode.Related decoder is specified in the config file as DECODER_##TYPE##")do |f|
		$opts[:type] = f
	end
	opt.on("-v","--verbose","Display the last lines of each decoded files") do |v|
		$opts[:v] = true
	end
	opt.on("-h","--help","help") do |h|
		puts opt_parser
	end
end

opt_parser.parse!

unless ARGV[0] && $opts[:type]
	$stderr.puts opt_parser if $opts[:v]
	abort
end
unless EMMConfig["DECODER_#{$opts[:type]}"] 
    $stderr.puts "No decoder for this type #{$opts[:type]} is specified in the config file" if $opts[:v]
	abort
end

$is_dir = false
if Dir.exists? ARGV[0]
	$is_dir = true
elsif !File.exists? ARGV[0]
	$stderr.puts "The NAME doesn't exists as file or directory." if $opts[:v]
	abort
end
	
# will contain the results of the decoding ,i.e. the number of cdr type
$results = {}
$dec_cmd = EMMConfig["DECODER_#{$opts[:type]}"]
$re = /([A-Z]{3,8})\s*:\s*(\d{1,6})/
#fill in the results hash with the different cdr of this file
def decode_file file
    base = File.basename(file)
    # add the timestamp
    $results[file] = { values: {}, timestamp: Util.cdr2stamp(base) }
	cmd = $dec_cmd + " " + file + " | tail -n 40"
	res = `#{cmd} `
    if !$?.success?
        $stderr.puts "Decoder failed."; 
        abort; 
    end;
	res.split("\n").each do |line|
		next unless line.match $re
		$results[file][:values][$1] = $2
	end
end

def decode_directory
	Dir.foreach(ARGV[0]) do |f|
		decode_file f
	end
end

out = $is_dir ? "directory" : "file"
$is_dir ? decode_directory : decode_file(ARGV[0])

puts JSON.pretty_generate($results)



