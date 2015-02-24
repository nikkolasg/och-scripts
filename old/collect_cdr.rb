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
# collect cdr according to the type

require 'optparse'
require './logger'
require './database'
require 'open3'
require 'json'
require './ruby_util'
require './util'
$opts = {}

opt_parse = OptionParser.new do |opt|
    opt.banner = "collect_cdr.rb TYPE [OPTIONS]"
    opt.separator ""
    opt.separator "TYPE is the type of CDR you want to collect. For now, by default it will collect from every sources it knows from the config file for this type of CDR"
    opt.on("-v","--verbose","Verbose output") do |v|
        $opts[:v] = true
    end

end

opt_parse.parse!

unless ARGV[0] 
    Logger.<<($0,"ERROR","Call without type")
    $stderr.puts "Error , no type specified" if $opts[:v]
    abort
end

$type = ARGV[0].upcase
unless EMMConfig["CDR_TYPES"].include? $type
    Logger.<<($0,"ERROR","Type specified not known")
    abort
end

# fetch infos for the type
# i.e. all cdr found
def fetch_files
    cmd = "fetch_cdr.rb list #{$type} -f -v"
    error = false
    json = nil
    Open3.popen3(cmd) do |stdin,stdout,stderr,thr|
        json = stdout.read
        if !thr.value.success?
            error = true
            $stderr.puts stderr.read if $opts[:v]
        end
    end 
    if error
        abort
    end

    json = JSON.parse(json, symbolize_names: true)
    json = RubyUtil::symbolize json 
    if !json
        Logger.<<($0,"ERROR","while parsing json output from fetch_cdr")
        abort
    end
    count = json.inject(0) { |col,(k,v)| col + v.inject(0) { |c2,h| c2 + h[:files].size } }
    Logger.<<($0,"INFO", "Found #{count} #{$type} files #{$json.keys.join(',')} ... " if $opts[:v]
    json
end

def fetch_files_db
    db = Database::Mysql.default
    files = {}
    table = EMMConfig["DB_TABLE_CDR_#{$type}"]
    db.connect do 
        query = "SELECT file_name,switch FROM #{table} WHERE processed=0;"
        res = db.query query
        res.each_hash do |row|
            switch = row['switch']
            if !files[switch]
                files[switch] = []
            end
            files[switch] << row['file_name']
        end
        Logger.<<($0,"INFO","Retrieved #{res.num_rows} entries from db...") 
    end
    files
end

## filter the new files (json) by
#removing thoses in dbf (list)
def filter json,dbf
    count = 0
    json.each do |host,switches|
        switches.each do |sw_h|
            if dbf.empty? || !dbf[sw_h[:name]]
                puts "No files found in db for #{sw_h[:name]} ..."
                count = count + sw_h[:files].size
                next
            end
            str =  "Switch: #{sw_h[:name]} => Before : #{sw_h[:files].size} ... "
            sw_h[:files] = sw_h[:files] - dbf[sw_h[:name]]
            str << "After :  #{sw_h[:files].size} files ..\n" 
            Logger.<<($0,"INFO",str)
            count = count + sw_h[:files].size
        end
    end
    Logger.<<($0,"INFO","Will keep only #{count} files to download ...")
    json
end

def download json
    cmd = "fetch_cdr.rb download #{$type} -f -v"
    error = nil
    gen = JSON.generate(json)
    Open3.popen3(cmd) do |stdin,stderr,stdout,thr|
        stdin.puts gen
        stdin.close
        stdout.gets
        error = !thr.value.success?
        if error
            error =  stderr.gets if $opts[:v]
        end
    end
    if error 
        $stderr.puts error
        abort
    end 
end

def insert json
    ijson = {}
    json.each do |host,switches|
        switches.each do |sw_h|
            sw_h[:files].each do |file|
                ijson[file] = { values: { file_name: file,
                                          switch: sw_h[:name] 
                                        }
                                }
            end
        end
    end
    table = EMMConfig["DB_TABLE_CDR_#{$type}"]
    cmd = "insert.rb -t #{table} -v -s"
    error = false
    out = nil
    Open3.popen3(cmd) do |stdin,stdout,stderr,thr|
        stdin.puts JSON.generate(ijson)
        stdin.close
        out = stdout.read    
        error = !thr.value.success? ? stderr.gets : nil
    end
    if error
        $stderr.puts error if $opts[:v]
        abort
    end
    Logger.<<($0,"INFO","Inserted #{out.split("\n").size} files in #{table}... ") 

end

json = fetch_files
dbjson = fetch_files_db
json = filter json,dbjson
download json
insert json
exit