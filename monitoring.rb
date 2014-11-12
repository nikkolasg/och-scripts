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

def main
    unless ARGV[0] && ARGV[1]
        $stderr.puts "Not enough argument ..."
        abort
    end

    Dir["parser/*"].each { |f| require "./#{f}" }
    # defines operation and subject
    $ope = ARGV[0].downcase.to_sym
    opts = $opts.clone
    argv = ARGV.clone
    case $ope
        ## shortcut for main operation, no need to specify type of operation
    when *OperationParser.actions
        OperationParser.parse(argv,opts)
    when :database
        argv.shift ## because here we specified the type of operation
        DatabaseParser.parse(argv,opts)
    when :directories
        argv.shift
        DirectoriesParser.parse(argv,opts)
    when :test
        argv.shift
        TestParser.parse(argv)
    when :usage
        puts "OPERATION ===>\n\t"
        puts OperationParser.usage.join("\n\t")
        puts "SETUP    ===>\n\t"
        puts SetupParser.usage.join("\n\t")     
    else
        $stderr.puts "Operation unknown :( #{$ope} " 
        abort
    end
end


main


