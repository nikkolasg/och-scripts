#!/usr/bin/ruby
#
# Process the collected cdr by type (example : by collect_mss_cdr)
# it looks for new file in db, find them in the FS (filesystem)
# decode them,process  them (i.e. put important info in the db),
# then store them in backup and mark them as processed.
#

require 'optparse'
require 'open3'
require './config'
require './datalayer'
require './util'
require './logger'

$opts = {}
opt_parse = OptionParser.new do |opt|
    opt.banner = "process_cdr.rb type [OPTIONS]"
    opt.separator ""
    opt.separator "TYPE is the type of cdr you want to process. TYPE must be\
                specified into the config file under CDR_TYPE"
    opt.on("-v","--verbose","Verbose output") do |v|
        $opts[:v] = true
    end
end
opt_parse.parse!
unless ARGV[0]
    $stderr.puts "No type specified !!" if $opts[:v]
    abort
end

if EMMConfig["CDR_TYPES"].include? ARGV[0].upcase
    $type = ARGV[0].upcase.to_sym
else
    $stderr.puts "Specified type doesn't exist in config file..." if $opts[:v]
    abort
end
unless ($table = EMMConfig["DB_TABLE_CDR_#{$type}"])
    $stderr.puts "No table specified for this type in config file.." if $opts[:v]
    abort
end
unless ($stats = EMMConfig["DB_TABLE_STATS_#{$type}"])
    Logger.<<($0,"ERROR","No stats table specified for this type...")
    abort
end
puts "Type: #{$type}, Table: #{$table}, Stats: #{$stats}" if $opts[:v]
## get files to be processed
## Hash[:folder] = [f1,f2,f3...]
def new_files
    db = Datalayer::MysqlDatabase.default
    table = Datalayer::GenericTable.new(db,$table)
    # generate the hash structure
    files = Util.folders($type).inject({}) {|col,f| col[f] = []; col}
    puts files.inspect
    db.connect do
        select = ["*"]
        where = { processed: 0 }
        res = table.search_and select,where  
        res.each_hash do |row|
            files[row["switch"]] << row["file_name"]
        end
        puts "Found #{res.num_rows} new files to process in #{$table}..." if $opts[:v]
    end
    files
end

#decode sequentially
def decode file
    cmd = "./decoder.rb -v -t #{$type} #{file} "
    json = nil
    Open3.popen3(cmd) do |stdin,stdout,stderr,thr|
       json = stdout.read 
       if !thr.value.success?
            Logger.<<($0,"ERROR","Error while decoding #{file}: #{stderr.read}")
            abort
        end
    end
    json
end

def insert json
    cmd = "./insert.rb -t #{$stats} -c" 
    file_inserted = ""
    error = nil
    Open3.popen3(cmd) do |stdin,stdout,stderr,thr|
        stdin.write json
        stdin.close

        file_inserted = stdout.read

        if !thr.value.success?
            error = stderr.read
        end
    end
    if error
        Logger.<<($0,"ERROR","Error while inserting ... #{error}")
        abort
    end
    file_inserted
end

def backup file
    cmd = "./backup_cdr.rb -t CDR_MSS"
    error = nil
    Open3.popen3(cmd) do |stdin,stdout,stderr,thr|
        stdin.write(file)
        stdin.close

        if !thr.value.success?
            error = stderr.read
        end
    end
    if error
        Logger.<<($0,"ERROR","Error while backup file #{file}.#{error}")
        abort
    end
end

##
# PROCESSING flow
# can be easily altered into a 
# multithreaded script
switches = new_files
# iterate over the differents switches = folders
switches.each do |sw,files|
    # get the full path for the files in  this folder
    path = Util.data_path(EMMConfig["DATA_STORE_DIR"],sw)
    files.each do |ff|
        #construct full path 
        f = path + "/" + ff
        print "#{f} ... " if $opts[:v]
        json = decode f
        print "Decoded / " if $opts[:v]
        file = insert json
        print "Inserted / " if $opts[:v]
        backup file
        print "Backed Up \/\n" if $opts[:v]
    end
end

exit
