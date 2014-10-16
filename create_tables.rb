
require './datalayer' # custom module for encapsulation of database
require './logger'
require './config/config'
module Tables
# parse a file containing the fields of a table 
# used for big table. i.e. MSS => 76 rows ! so better in a separate file
# file must be in format 
# FIELD:MysqlType
# ex: calling_number:CHAR(10)
# return hash with field => mysql text
def self.create_table_records flow
    database = Datalayer::MysqlDatabase.default
    table = flow.table_records
    hash = flow.records_fields
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
        
def self.create_table_cdr flow

	database = Datalayer::MysqlDatabase.default
    database.connect do 
        table = flow.table_cdr
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

end
