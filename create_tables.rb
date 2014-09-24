#!/usr/bin/ruby

require 'mysql'
require './info'
require './datalayer' # custom module for encapsulation of database
require './logger'
require './config'
def create_tables

	database = Datalayer::MysqlDatabase.new(EMMConfig['DB_HOST'],
						EMMConfig['DB_NAME'],
						EMMConfig['DB_LOGIN'],
						EMMConfig['DB_PASS'])
	database.connect do 
		Logger.<<($0,"INFO","Connected to " + database.info.to_s)

		name = EMMConfig['DB_TABLE_MSS_CDR']
		retr_table = Datalayer::GenericTable.new(database,name)
		retr_table.query ("CREATE TABLE IF NOT EXISTS \
				#{name}( \
				id INT NOT NULL AUTO_INCREMENT,
				file_name VARCHAR(40) NOT NULL UNIQUE,
				processed BOOLEAN DEFAULT 0,
				time TIMESTAMP,
				PRIMARY KEY (id));")
		# TO SEE if needed.
		# maybe on processed also
		retr_table.query("CREATE INDEX processed_index
				ON #{name} (file_name);")
		retr_table.describe
	end
end



if create_tables
	puts "Success !"
	exit
else
	puts "Error, exit."
	abort
end
