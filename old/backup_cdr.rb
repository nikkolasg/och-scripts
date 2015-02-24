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
#
# Mark the files input as processed in the db
# and move them and zip them for backup
# ##
# ## TODO make it guess the right table
require 'optparse'
require './config'
require './database'
require './util'

$opts = {}

opt_parse = OptionParser.new do |opt|
    opt.banner = "back_up.rb INPUT OPTIONS"
    opt.separator ""
    opt.separator "INPUT can be a file or pipe. List of file names to be processed,separeted by newline. it must be relative path or complete path, not just filename.At least with the parent folder."
    opt.on("-t","--table TABLE","TABLE on where to specify the processed bit") do |t|
        $opts[:table] = t
    end
    opt.on("-v","--verbose","Verbose output") do |v|
        $opts[:v] = true
    end
end
opt_parse.parse!
$input = $stdin.read

unless $input && $opts[:table] && EMMConfig["DB_TABLE_#{$opts[:table]}"]
    $stderr.puts "No input or (wrong) table specified." if $opts[:v]
    abort
end

$files = $input.split("\n")
unless $files.size > 0
    $stderr.puts "No files found ... "
    abort
end
# mark them processed in the specified table
# i.e. put 1 in processed column
def process db
   #verify if table exists
   table = EMMConfig["DB_TABLE_#{$opts[:table]}"]
   sql="UPDATE  #{table} SET processed=1 WHERE file_name IN "
   sql << "(" << $files.map{ |k| "'#{File.basename(k)}'" }.join(',') << ");"
   puts sql if $opts[:v]
   db.query(sql)
end

# move the files to the back_up dir
# and compress them
def store_backup
    ret = true
    base = Util.data_path(EMMConfig["DATA_BACK_DIR"])
    $files.each do |f|
        # directory of the file 
        # LSMSS10, LSMSS30 , etc
        dir = File.basename(File.expand_path("#{f}/.."))
        back_path = base + "/" + dir + "/" + File.basename(f)
        cmd = "mv #{f} #{back_path}"
        unless system(cmd)
           $stderr.puts "Move #{cmd} did not work ... Next" if $opts[:v]
            ret = false
            next
        end

        # compress the file
        cmd = EMMConfig["COMPRESS"] + " #{back_path}"
        unless system(cmd)
            $stderr.puts "Compress #{cmd} did not work ... Next" if $opts[:v]
            ret = false
            next
        end
    end    
    ret
end

begin

    db = Database::Mysql.default
    db.connect do 
        process db
    end

    if store_backup
        exit
    else
        abort
    end
rescue => e
    $stderr.puts e.message if $opts[:v]
    abort
end