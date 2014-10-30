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
require_relative 'config/config.rb'

# direction by default is both
$opts = { dir: :input }
opt_parse = OptionParser.new do |opt|
    opt.banner = "monitor.rb OPERATION SUBJECT [OPTIONS]"
    opt.on("-v","--verbose","verbose output") do |v|
        $opts[:v] = true
    end
    opt.on("-d","--direction DIR","For BOTH options (all,flow), a direction can be appended : input or output or both. By default it is both.
                                It specifies from which direction you want to process a flow. ") do |d|
        $opts[:dir] = d.downcase.to_sym
    end
    opt.on("-m","--monitor MONITOR","If you are doing a monitoring operation, you can specify which monitors you'd like (regarding the flow)") do |d|
        $opts[:monitor] = d
    end

end
opt_parse.parse!
def main
    unless ARGV[0] && ARGV[1]
        $stderr.puts "Not enough argument ..."
        abort
    end
    
    Dir["parser/*"].each { |f| require "./#{f}" }
    # defines operation and subject
    $ope = ARGV[0].downcase.to_sym
   ## redirect to the right action
    Util::starts_for $opts[:dir] do |dir|
        opts = $opts.clone
        opts[:dir] = dir
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
        $stderr.puts "Operation unknown :(" 
        abort
    end
    end
end 


main


