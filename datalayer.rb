module Datalayer

	class Database

	require 'mysql'
	def initialize(info)
		@host = info[:host]
		@db = info[:db]
		@name = info[:name]
		@pass = info[:pass]
		@con = nil
	end
			

	def connect
		safe_query do 
			@con = Mysql.new(  @host,@name,@pass,@db)
			puts "Logged into #{@db} at #{@name}@#{@host}"
		end
	end
	
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
		safe_query do
			 puts "Database info : #{@con.get_server_info}"
			 puts @con.query("describe #{table};") if table  
		end
	end


	## Wrapper that handles exception
	def safe_query()
		ret = yield
		return ( ret ? ret : true ) # return not nil, otherwise return true
		rescue Mysql::Error => e
			STDERR.puts "#{Time.now} : #{e.errno}, #{e.error}"
			return 
	end
end
end
