#!/usr/bin/ruby
# Monitor is the wrapper layer of all operations
# possible. 
# it can 
# GET a flow, a specific flow, or files in a folder
# INSERT flow, flow or files
# PROCESS flow, flow, files 
# RUN flow,flow or files
# TEST ... todo
require 'optparse'
require './config'

# direction by default is both
$opts = {  }
opt_parse = OptionParser.new do |opt|
    opt.banner = "monitor.rb OPERATION SUBJECT [OPTIONS]"
    opt.on("-v","--verbose","verbose output") do |v|
        $opts[:v] = true
    end
    opt.on("--direction DIR","For BOTH options (all,flow), a direction can be appended : input or output or both. By default it is both.
                                It specifies from which direction you want to process a flow. ") do |d|
        $opts[:dir] = d.downcase.to_sym
    end
    opt.on("--monitor MONITOR","If you are doing a monitoring operation, you can specify which monitors you'd like (regarding the flow)") do |d|
        $opts[:monitor] = d
    end
    opt.on("--take NUMBER","If you want to take only certain number of records for each switch ,you can specifiy here") do |t|
        $opts[:take] = t.to_i
    end
    opt.on("--min-date DATE","If you want to select files that are only more recent than the date. FORMAT = ") do |d|
        $opts[:min_date] = d
    end
    opt.on("--max-date DATE","If you want to select files that are only less recent than the date. FORMAT specified by the GNU `date` command.") do |d|
        $opts[:max_date] = d
    end
end

def defaults
    $opts[:dir] = :both
    # $opts[:monitor] = all  !! NO monitor by default
end
defaults
opt_parse.parse!

def main2 args,opts
    argv = args.clone
    opt = opts.clone
    opt[:argv] = argv.clone ## so parser can still get the full line if needed
    RubyUtil::require_folder("parser")
    #Dir["parser/*"].each { |f| require "./#{f}" }
    
    word = argv.shift.downcase.to_sym
    
    Parser::constants.each do |parser|
        next unless Parser::const_get(parser)::KEYWORDS.include? word
        Parser::const_get(parser).parse(argv,opt)
        break # only one operation at a time !
    end
    
    Logger.<<(__FILE__,"INFO","Closing monitoring tool ...")
end

main2 ARGV,$opts


