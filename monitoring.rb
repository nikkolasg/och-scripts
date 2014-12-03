#!/usr/bin/ruby
# Monitor is the wrapper layer of all operations
# TEST ... todo
require 'optparse'
require_relative 'config'

$opts = {  }
opt_parse = OptionParser.new do |opt|
    opt.banner = "monitor.rb OPERATION SUBJECT [OPTIONS]"
    opt.on("-v","--verbose","verbose output") do |v|
        $opts[:v] = true
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
    opt.on("-u","--union","For processing, you want to take the union table of the sources you are processing on. By default it is NOT enabled.") do |d|
        $opts[:union] = true
    end
    opt.on("-d","--debug","IF you want debugging capabilities (mostly on decoder for now).") do |d|
        $opts[:d] = true
    end
end

opt_parse.parse!

def main args,opts
    argv = args.clone
    opt = opts.clone
    opt[:argv] = argv.clone ## so parser can still get the full line if needed
    Dir[File::dirname(__FILE__) + "/parser/*.rb"].each do |f|
        require_relative "parser/#{File::basename(f)}" 
    end
    
    word = argv.shift.downcase.to_sym
    
    Parser::constants.each do |parser|
        next unless Parser::const_get(parser)::KEYWORDS.include? word
        Parser::const_get(parser).parse(argv,opt)
        break # only one operation at a time !
    end
    
    Logger.<<(__FILE__,"INFO","Closing monitoring tool ...")
end

main ARGV,$opts


