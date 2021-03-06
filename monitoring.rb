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
    opt.on("--take NUMBER","If you want to take only certain number of records for each folder ,you can specifiy here. You can use this for debuging,quickly retrieve file, or if you just want a sample") do |t|
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
        SignalHandler.debug
    end
    opt.on("--nb-line LINE","You can stop the decoding of a file at a certain line. Can be useful for dumping big file fastly") do |n|
        $opts[:nb_line] = n.to_i
    end
    opt.on("--mock","If you are doing a fix or a setup operation, you can see what the tool would actually be doing, but without doing the operations. Can be useful to see if the operation is really what you want") do |n|
        $opts[:mock] = true
    end
    opt.on("--insert-only","If you only want to insert files and look in the db after, but WITHOUT any processing on theses records (old ones for example), you can specify it here. It will simply marks the files it inserts as already processed in the monitors sections. OPTIONS BETA. NOT TESTED so be aware.") do |n|
        $opts[:insert_only] = true
    end
    opt.on("--no-fetch-all","Add this optioon if you think that the MySql query will have millions of results and could not fit into memory. It will trigger one record at a time and let the ruby gc work it out for you.") do |n|
        $opts[:sql_no_fetch_all] = true
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
SignalHandler.enable { Logger.<<(__FILE__,"INFO","Signal handler SIGINT enabled.")}

# TODO
def lock 
    
    fname = File.join(File.dirname(__FILE__),"locks",ARGV.join('-')) + ".LOCK"
    File.open(fname,File::RDWR|File::CREAT,0644) do |f|
        unless f.flock( File::LOCK_NB | File::LOCK_EX )
            Logger.<<(__FILE__,"INFO","Instance already running. Abort.")
            f.close
            exit
        end
        SignalHandler.ensure_block { f.close }
        main ARGV,$opts
    end
end

lock
