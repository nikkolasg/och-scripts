module Database
    require_relative '../logger'	
    require_relative '../config'
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

    class Mysql
        CONNECTION_TRY = 5
        attr_reader :con
        @@default_db = nil   
        def self.default opts = {}
            db = Conf.database
            Mysql.new(db.host,db.name,db.login,db.password,opts)
        end
        def initialize(h,db,login,pass,opts = {})
            @host = h
            @db = db
            @login = login
            @pass = pass
            @con = nil
            @opts = opts
        end

        def connect_
            @con = ::Mysql.new(@host,@name,@pass,@db) unless @con
            return test_connection
        end
        def test_connection
            begin
                return false unless @con
                @con.ping()
            rescue ::Mysql::Error => e
                Logger.<<(__FILE__,"WARNING","Database not responding to ping...")
                return false
            end
            return true
        end


        ##
        ## Main method, must call every other method inside this method
        def connect
            safe_query do 
                if @con ## if we already are connected
                    yield
                else
                    i = 0
                    connected = false
                    while (i < CONNECTION_TRY && !connected) do 
                        connected = connect_
                        i = i + 1
                    end
                    unless connected
                        Logger.<<(__FILE__,"ERROR","Can not connect to DB ..Abort.")
                        abort
                    end
                    Logger.<<(__FILE__,"INFO","Logged into #{@db} at #{@name}@#{@host}") if @opts[:v]
                    yield
                    @con.close if @con
                    @con = nil
                    Logger.<<(__FILE__,"INFO","Logged OUT from  #{@db} at #{@name}@#{@host}") if @opts[:v]
                end
            end
        end
        # useless for now	
        def close
            safe_query do
                @con.close if @con 
            end
        end
        def  query(sql_query)
            #puts "#{@con}"
            @con.query(sql_query)  
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
            ret = yield if block_given?
            return ( ret ? ret : true ) # return not nil, otherwise return true
        rescue ::Mysql::Error => e
            Logger.<<(__FILE__,"ERROR","#{e.errno}, #{e.error}")
            raise e 
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

    class SqlGenerator

        def self.for_records table_name,records_fields
            sql =  "CREATE TABLE IF NOT EXISTS #{table_name} (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT,
        file_id INT UNSIGNED NOT NULL,
            #{ records_fields.map { |k,v| "#{k} #{v}"}.join(',') },
        PRIMARY KEY (id),
        INDEX (file_id)) "
            sql += " ENGINE=MYISAM"
            sql
        end

        def self.for_records_union table_name,records_fields,opts
            sql =  "CREATE TABLE IF NOT EXISTS #{table_name} (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT,
        file_id INT UNSIGNED NOT NULL,
            #{ records_fields.map { |k,v| "#{k} #{v}"}.join(',') },
        INDEX (id),
        INDEX (file_id)) "
            sql += "ENGINE=MERGE UNION=(#{opts[:union].join(',')}) INSERT_METHOD=NO"
            sql
        end

        def self.for_files table_name
            length = 40
            print "PLease, specify length of cdr file name (enter to default 40) : "
            v = STDIN.gets.chomp
            length = v.empty? ? length : v.to_i
            sql = "CREATE TABLE IF NOT EXISTS 
            #{table_name}( 
                file_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
                file_name VARCHAR(#{length}) NOT NULL UNIQUE, 
                processed BOOLEAN DEFAULT 0,
                folder VARCHAR(10) DEFAULT '',
            #{Conf::database.timestamp} TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX(processed),
            INDEX(file_name),
            PRIMARY KEY(file_id)) "
            sql += "ENGINE=MYISAM"
            sql
        end

        def self.for_files_union table_name,opts
            sql = "CREATE TABLE IF NOT EXISTS 
            #{table_name}( 
                file_id INT NOT NULL UNSIGNED AUTO_INCREMENT,
                file_name CHAR(40) NOT NULL , 
                processed BOOLEAN DEFAULT 0,
                folder VARCHAR(10) DEFAULT '',
            #{Conf::database.timestamp} TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX(processed),
            INDEX(file_id),
            INDEX(file_name)
                ) "
            sql += "ENGINE=MERGE UNION=(#{opts[:union].join(',')}) INSERT_METHOD=NO"
            sql
        end

        ## TODO
        def self.for_monitor_stats table_name,columns
            sql = "CREATE TABLE IF NOT EXISTS #{table_name}"
            sql += "(#{Conf::database.timestamp} INT UNSIGNED UNIQUE DEFAULT 0, "
            sql += columns.map {|c| "#{c} INT UNSIGNED DEFAULT 0" }.join(',')
            sql += ", PRIMARY KEY (#{Conf::database.timestamp})) ENGINE=MYISAM"
            sql
        end
        def self.for_monitor_union
            raise "No monitor MERGE table implemented yet."
        end

        def self.for_monitor_source table_name
            sql =  "CREATE TABLE IF NOT EXISTS #{table_name} (" +
                " file_id INT UNSIGNED NOT NULL," +
                " #{Conf::database.timestamp} TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                " PRIMARY KEY (file_id))" 
            sql += " ENGINE=MYISAM"
            sql

        end
        def self.for_monitor_source_backlog table_name
            sql = "CREATE TABLE IF NOT EXISTS #{table_name} (" +
                " file_id INT UNSIGNED NOT NULL AUTO_INCREMENT, " +
                " file_name VARCHAR(40) UNIQUE, " +
                " PRIMARY KEY (file_id)) " +
                " ENGINE=MYISAM "
            sql
        end

        def self.for_monitor_records_union table_name,opts
            sql =  "CREATE TABLE IF NOT EXISTS #{table_name} (" +
                " file_id INT UNSIGNED NOT NULL,"+
                " #{Conf::database.timestamp} TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                " INDEX(file_id)) " 
            sql += "ENGINE=MERGE UNION=(#{opts[:union].join(',')} INSERT_METHOD=NO"
            sql
        end
    end
end
