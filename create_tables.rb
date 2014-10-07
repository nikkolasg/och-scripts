#!/usr/bin/ruby

require 'mysql'
require './datalayer' # custom module for encapsulation of database
require './logger'
require './config'

# parse a file containing the fields of a table 
# used for big table. i.e. MSS => 76 rows ! so better in a separate file
# file must be in format 
# FIELD:MysqlType
# ex: calling_number:CHAR(10)
# return hash with field => mysql text
def parse_table_file file
    hash = {}
    File.read(file).split("\n").each do |line|
        field,sql = line.split ':'
        hash[field] = sql
    end
    hash
end

def create_table_mss_records
    file = EMMConfig["MSS_RECORDS_FIELDS_FILE"]
    hash = parse_table_file file
    database = Datalayer::MysqlDatabase.default
    table = EMMConfig["DB_TABLE_MSS_RECORDS"]
    database.connect do 
        sql =
        "CREATE TABLE IF NOT EXISTS #{table} (
        id INT NOT NULL AUTO_INCREMENT,
        file_name CHAR(38) NOT NULL,
        switch CHAR(10) NOT NULL,
        name CHAR(10) NOT NULL,
        processed BOOLEAN DEFAULT 0,
        #{ hash.map { |k,v| "#{k} #{v} "}.join(',') },
        PRIMARY KEY (id));"
        puts sql if $opts[:v]
        database.query(sql)
    end
end
        
        




def create_table_mss_cdr

	database = Datalayer::MysqlDatabase.default
    database.connect do 

		name = EMMConfig['DB_TABLE_MSS_CDR']
		sql = "CREATE TABLE IF NOT EXISTS 
				#{name}( 
				id INT PRIMARY KEY AUTO_INCREMENT,
				file_name VARCHAR(40) NOT NULL UNIQUE,
				processed BOOLEAN DEFAULT 0,
                switch VARCHAR(10),
				timest TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);"
        puts sql if $opts[:v]
        database.query(sql)
		# TO SEE if needed.
		# maybe on processed also
		database.query("CREATE INDEX processed_index
				ON #{name} (processed);")
        

	end
end

def create_table flow,table
    case flow
    when "MSS"
        case table
        when "CDR"
            create_table_mss_cdr
        when "STATS"
            create_table_mss_stats
        when "RECORDS"
            create_table_mss_records
        end
    end

end


require 'optparse'

$opts = {}
OptionParser.new do |opt|
    opt.banner = "create the differents table used by monitor tool"
    opt.separator "create_tables.rb flow table"
    opt.separator "flow is for which flow you want to create (mss,sms...) and table is which table for this flow (cdr,records,stats)"
    opt.on("-v","--verbose","Verbose output") do |v|
        $opts[:v] = true
    end
end.parse!

unless ARGV[0] && ARGV[1]
    raise "No arguments given ..."
    abort
end
flow = ARGV[0].upcase
table = ARGV[1].upcase
unless (config_table = EMMConfig["DB_TABLE_#{flow}_#{table}"]) 
   raise "Did not find any matching table in config file !"
  abort
end

create_table flow,table
exit

