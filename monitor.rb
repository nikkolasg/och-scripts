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
$opts = {}
opt_parse = OptionParser.new do |opt|
    opt.banner = "monitor.rb OPERATION SUBJECT [OPTIONS]"
    opt.separator ""
    opt.separator "OPERATION :  get, fetch the subject and put into the server/ folders,register them into the db.Does not get twice the same file indeed if already present in the db."
    opt.separator "             insert, decode & insert the subject into the cdr data tables.It is the insertion of the decoded raw data"
    opt.separator "             process, process the subject,i.e. make the stats, aggregation.."
    opt.separator "             run, make the three previous operation in a row for the subject."
    opt.separator "             test, launch the various test available. not finished. SUBJECT LIST for test => refer to test.rb"
    opt.separator "SUBJECT :    all, represents all flows available"
    opt.separator "             flow MSS, flow DATA, flow MMS, etc. operate only on this flow"
    opt.separator "             files DIRECTORY, subject is all the files contained in DIRECTORY. Special operations are done, careful.Backlog operations, to debug, new functionalities etc"
    opt.on("-v","--verbose","verbose output") do |v|
        $opts[:v] = true
    end
    opt.on("-d","--direction DIR","For BOTH options (all,flow), a direction can be appended : input or output or both. By default it is both.
                                It specifies from which direction you want to process a flow. ") do |d|
        $opts[:dir] = d
    end

end
opt_parse.parse!
def main
    unless ARGV[0] && ARGV[1]
        $stderr.puts "Not enough argument ..."
        abort
    end

    # defines operation and subject
    $ope = ARGV[0].downcase.to_sym
    $sub = ARGV[1].downcase.to_sym

    ## check the argument for the subject
    case $sub 
    when :all
        #do nothing
    when :flow
        ($stderr.puts "No flow specified";abort;) unless ARGV[2]
        $flow = App.flow(ARGV[2].upcase.to_sym)
        ($stderr.puts "Flow does not exists in config";abort;) unless $flow
    when :files
        if ARGV[2]
            fold = ARGV[2]
            if fold[-1] == "/"
               fold = fold[0..-1]
            end 
            $folder = fold
            $files = Dir.glob($folder+"/*")
        elsif (!$stdin.tty? && files = $stdin.read)
            $files = files.split("\n")
            $folder = File.dirname($files.first)
            $files = $files.map { |f| File.basename(f) }
        else
            $stderr.puts "Wrong args for files subject ..."
            abort
        end
    end
    ## redirect to the right action
    case $ope
    when :get
        get
    when :insert
        insert
    when :process
        process
    when :test
        test
    else
        $stderr.puts "Operation unknown :(" 
        abort
    end
end 

def insert
    require './insert/inserter'
    infos = {}
    infos[:v] = $opts[:v]
    infos[:dir] = :input
    str = "Command INSERT operations on "
    if $sub == :all
        str << "all flows !"
        puts str if $opts[:v]
        flows = App.flows
        flows.each do |f|
            Insert.create(f.upcase.to_sym,infos).insert
        end
    elsif $sub == :flow
        str << "flow #{$flow}..."
        puts str if $opts[:v]
        Inserter.create($flow,infos).insert
    elsif $sub == :files
        str << "files #{File.dirname($files.first)} ..."
        puts str if $opts[:v]
        infos[:files] = $files
        Inserter.create(:files,infos).insert
    end
end

def get
    require './get/getter'
    infos = {}
    str = "Command GET operations on "
    # add the direction of the flow we want to get
    infos[:dir] = :input 
    infos[:v] = $opts[:v]
    if $sub == :all
        str << "all flows !"
        puts str if $opts[:v]
        flows = App.flows
        flows.each do |f|
            Getter.create(f,infos).get
        end
    elsif $sub == :flow
        str << "flow #{$flow.name} ..."
        puts str if $opts[:v]
        Getter.create($flow, infos).get
    elsif $sub == :files
        str << " #{$folder} .."
        puts str if $opts[:v]
        infos[:files] = $files
        infos[:folder] = $folder if $folder
        Getter.create(:files, infos).get
    end
end

main


