#!/usr/bin/ruby

require 'mysql'
require './info'
require './datalayer' # custom module for encapsulation of database


# call and connect to the db
def create_retrieved_cdr_table()
	table_name = "retrieved_cdr_table"
	database = Datalayer::Database.new(Info.db)
	return unless database.connect
	database.query do | con |
		# send return to the yield
		 con.query("CREATE TABLE IF NOT EXISTS \
			#{table_name}( \
				id INT NOT NULL AUTO_INCREMENT,
				file_name VARCHAR(40) NOT NULL UNIQUE,
				last_seen DATE,
				is_done BOOLEAN DEFAULT NULL,
				PRIMARY KEY (id));")	
	
		con.list_fields(table_name)
	end
	database.info
	database.close
end


if create_retrieved_cdr_table
	puts "Success !"
	exit
else
	puts "Error, exit."
	abort
end
