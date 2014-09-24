#!/usr/bin/ruby
##
#   Insert data into database stats table from input
#   The input is categorized by a timestamp, 
#   if the timestamp already exists in the database,
#   it adds to the already-stored values
#

require 'optparse'
require './config'
require './datalayer'
require 'json'

$opts =  {}
opt_parser = OptionParser.new do |opt|
    opt.banner =  "insert.rb  INPUT OPTIONS"
    opt.separator ""
    opt.separator "INPUT can be passed as pipe or file input (<).\
                    It consists of a JSON structure. Usally, looks like \
                    { entry1: { timestamp: \"2014blablabla\", \
                            values : { \
                                FOW:314,\
                                MOC:2001\
                                }\
                            }\
                    }\
                    timestamp must be a unix timestamp"
                    opt.on("-t","--table TABLE","table where to put the data") do |t|
                        $opts[:table] = t
                    end
                    opt.on("-v","--verbose","verbose output") do |v|
                        $opts[:v] = true
                    end
                    opt.on("-c","--cumul","Verify if new rows are duplicate, and if so cumul the ancient row with the new row (simple addition). CAREFUL: if you DONT specify this options,you might get an error with duplicate from the DB if you have a UNIQUE column") do |c|
                        $opts[:c] = true
                    end
                    
end


def insert db
    strQuery = "INSERT INTO #{$opts[:table]}"
    $fields.each_key do |k|
        s = strQuery.clone
        s << "(" << $fields[k][:values].keys.join(',') 
        s << ",#{EMMConfig['DB_TIMESTAMP']}) VALUES " 

        s << "(" << $fields[k][:values].values.join(',') 
        s << ", #{$fields[k][:timestamp]} ) "
        if $opts[:c] 
            s << " ON DUPLICATE KEY UPDATE "
            # make the update on a existing row
            s << $fields[k][:values].map { |k,v| "#{k}=#{k}+#{v}"}.join(',')
        end
        s << ";"
        puts s if $opts[:v]
        db.query(s) 
        ## mark it as processed
        $fields[k][:p] = true
    end
end

def filter
    # keep only fields contained in the database
    # theses fields are desribed in config.cfg
    unless ($db_fields = EMMConfig["#{$opts[:table]}_FIELDS"]) 
        $stderr.puts "No fields specified for this table (#{$opts[:table]}) in the config file"
        abort
    end
    puts "Fields to be filtered: " + $db_fields.join(',') if $opts[:v]
    $fields.each_key do |k|
        # transform into symbols so addressing is easier & faster
        $fields[k] = $fields[k].inject({}) { |col,(k,v)| col[k.to_sym] = v; col }
        # keep only the fields we know
        $fields[k][:values].select! { |k,v| $db_fields.include? k.to_s }
    end
end

def parse_fields
    begin
        $fields = JSON.parse($input)
        puts "JSON parsed." if $opts[:v]
    rescue => e
        $stderr.puts e.message
        abort
    end 
end

## File have been decoded & inserted, so
opt_parser.parse!
puts "reading input ..." if $opts[:v]
$input = $stdin.read

unless $opts[:table] && $input && !$input.empty?
    puts "bad input or not table specified"
    puts opt_parser
    abort
end

$fields = {}

parse_fields
puts "BEFORE filter: \n" + $fields.inspect if $opts[:v]
filter
puts "AFTER filter: \n" + $fields.inspect if $opts[:v]
begin
    db = Datalayer::MysqlDatabase.default
    db.connect do 
        insert db
    end
rescue => e
    $stderr.puts e.message if $opts[:v]
    abort
end

$stdout.puts $fields.each_key.to_a.join("\n")
exit

