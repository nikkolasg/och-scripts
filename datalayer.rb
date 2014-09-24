module Datalayer
require './logger'	
require 'set'
	require 'mysql'
	class Mysql::Result
		def pretty
			r = self.fetch_fields.collect { |f| f.name }.join("\t")
			r << "\n"
			self.each do |row|
				r << row.join("\t") << "\n"
			end
			r << "\nRetrieved #{self.num_rows} .."
			r
		end
	end

	class MysqlDatabase
    @@default_db = nil   
    def self.default
         MysqlDatabase.new(EMMConfig['DB_HOST'],
                              EMMConfig['DB_NAME'],
                              EMMConfig['DB_LOGIN'],
                              EMMConfig['DB_PASS'])
    end
	def initialize(h,db,login,pass)
		@host = h
		@db = db
		@login = login
		@pass = pass
		@con = nil
	end
			
	##
	## Main method, must call every other method inside this method
	def connect
		safe_query do 
			@con = Mysql.new(  @host,@name,@pass,@db) unless @con
			Logger.<<($0,"INFO","Logged into #{@db} at #{@name}@#{@host}")
			yield
			@con.close if @con
            Logger.<<($0,"INFO","Logged OUT from  #{@db} at #{@name}@#{@host}")

		end
	end
	# useless for now	
	def close
		safe_query do
			 @con.close if @con 
		end
	end

	def  query(sql_query)
        safe_query do 
		    @con.query(sql_query)  
        end
	end

	# display basic info on the database
	# and on a table if specified
	def info(table = nil)
		 info = "Database info : #{@con.get_server_info}"
		 info << @con.query("describe #{table};") if table  
		 info
	end


	## Wrapper that handles exception
	## and return the result for the query or true if no(result ^ exception)
	def safe_query()
		ret = yield
		return ( ret ? ret : true ) # return not nil, otherwise return true
		rescue Mysql::Error => e
			Logger.<<($0,"CRITICAL","#{e.errno}, #{e.error}")
			return 
	end

	

	end
	## Abstraction of a mysql table
	## can be used for simple table where queries are relatively simple
	class GenericTable
		attr_reader :table_name	
		def initialize(db,table_name)
			@db = db
			@table_name = table_name
		end
		## TO USE WITH CAUTIONS
		## just needed for things like, create or alter
		## the goal is not to design a whole DAO library...§
		def query sql 
			@db.query(sql)
		end
		
		def describe
			@db.query("describe #{@table_name};")
		end
		## Insert the values in the table
		## h is expected to be a Hash with
		## key : name of oolumn
		## value : value to be inserted
		def insert h
			sql = "INSERT into #{@table_name} "
			sql << "(" << h.keys.join(',') << ")"
			sql << " VALUES (" << h.values.join(',') << ");"
			## TODO return value here ?
			@db.query(sql)
		end
		## insert multiple row at a time
		## h is hash WHEN MULTIPLE FIELDS 
		## h[:fields] = [  fields name to insert ]
		## h[:values] = [ [fields values],[fields values] ... ]
		## h is hash WHEN ONE FIELD
		## h[:fields] = field name
		## h[:values] = [ value1,v2,v3...]
		def insert_multiple h
			sql = "INSERT into #{@table_name} "
			if h[:fields].is_a?(Array)
				sql << "(" << h[:fields].join(',') << ")"
				sql << " VALUES " << h[:values].map{|v| "(" + v.map{ |vv| "'#{vv}'" }.join(',') + ")" }.join(',') << ";"
			# only one field
			else 
				sql << "(" << h[:fields] << ")"
				sql << " VALUES " << h[:values].map{|v| "('"+v.to_s + "')"}.join(',') << ";"
			end
			@db.query(sql)
		end
		## update a single row of the table based on the id column
		## key is expected to be a tuple ==> array of 2 elements
		## key[0] => column name which is the primary key of the table
		## key[1] => value of the primary key
		## h is expected to be a hash, to update a SINGLE record 
		## key : name of the column. 
		##	search for the record
		## value : value to be updated
		def update_single key, h
			sql = "UPDATE #{@table_name} SET "
			sql << h.map { |k,v| k.to_s << "='" << v.to_s << "'" }.join(",")
			sql << "WHERE #{key[0].to_s}='#{key[1].to_s}';"
			@db.query(sql)
		end
			
		## Search the database for multiple rows specifying
		## all conditions to be true
		## select :  an array of selected columns
		## where : an hash of value to put in where clause 
		##		related with AND
		##	key : name of column
		## 	value : single value
		##		OR array, will be put in a IN clause

		def search_and select,where
			sql = "SELECT " << select.join(',') 
			sql << " FROM #{@table_name}"
			whereAnd = where.map do |k,v| 
				
				if v.is_a?(Array) || v.is_a?(Set)
					k.to_s << " IN ( " << v.collect!{|a| "'#{a}'"}.join(',') << ")"
				else
					k.to_s << "='" << v.to_s << "'"
				end
			end.join(' AND ')
			if whereAnd.length > 0 
				sql << " WHERE " << whereAnd 
			end
			sql << ";"
			@db.query(sql)
		end
	end
end
