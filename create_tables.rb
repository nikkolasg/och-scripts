
require './datalayer' # custom module for encapsulation of database
require './logger'
require './config/config'
module Datalayer
    class Tables
        # parse a file containing the fields of a table 
        # used for big table. i.e. MSS => 76 rows ! so better in a separate file
        # file must be in format 
        # FIELD:MysqlType
        # ex: calling_number:CHAR(10)
        # return hash with field => mysql text
        def self.create_table_records flow
            sql = lambda { |table,hash| 
                "CREATE TABLE IF NOT EXISTS #{table} (
        id INT NOT NULL AUTO_INCREMENT,
        file_id CHAR(38) NOT NULL,
        switch CHAR(10) NOT NULL,
        name CHAR(10) NOT NULL,
        processed BOOLEAN DEFAULT 0,
                #{ hash.map { |k,v| "#{k} #{v} "}.join(',') },
        PRIMARY KEY (id));"
            }

            database = Datalayer::MysqlDatabase.default
            table = flow.table_records
            table_out = flow.table_records(:output)

            hash = flow.records_fields
            database.connect do 
                database.query(sql.call(table,hash))
                database.query(sql.call(table_out,hash))
                self.create_index db,table,"processed"
                self.create_index db,table_out,"processed"
                self.create_index db,table,"file_id"
                self.create_index db,table_out,"file_id"
            end
        end

        def self.create_table_cdr flow
            sql =lambda { |table|
                "CREATE TABLE IF NOT EXISTS 
                #{table}( 
                id INT PRIMARY KEY AUTO_INCREMENT,
                file_name CHAR(40) NOT NULL UNIQUE, 
                processed BOOLEAN DEFAULT 0,
                switch VARCHAR(10),
                timest TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );"}

            database = Datalayer::MysqlDatabase.default
            database.connect do 
                table = flow.table_cdr
                database.query(sql.call(table))
                table_out = flow.table_cdr(:output)
                database.query(sql.call(table_out))
                # TO SEE if needed.
                # maybe on processed also
                self.create_index db,table,"processed"
                self.create_index db,table_out,"processed"

            end
        end
        def self.create_table_monitors flow
            db = Datalayer::MysqlDatabase.default
            db.connect do 
                flow.monitors.each do |m|
                    sql = Tables::create_table_monitor m,db
                end
            end
        end
        def self.create_table_monitor monitor,db = nil
            sql = "CREATE TABLE IF NOT EXISTS #{monitor.table}"
            sql += "(#{App.database.timestamp} INT UNSIGNED UNIQUE DEFAULT 0, "
            maps = monitor.filter_records.map do |rec|
                [ monitor.column_record(rec,:input) + " INT DEFAULT 0 ",
                  monitor.column_record(rec,:output) + " INT DEFAULT 0" ]
            end.flatten(1).join(',')
            sql += maps
            sql += ");"
            if db 
                db.query(sql)
            else
                db =Datalayer::MysqlDatabase.default
                db.connect do 
                    db.query(sql)
                end
            end
        end
        def self.create_index db,table,column
            unless self.index_exists? db,table,column
                db.query (" ALTER TABLE #{table} ADD INDEX (#{column});")
            end
        end
        def self.index_exists? db,table,column
            sql = "show index from #{table} where Column_name = '#{column}';"
            res = db.query(sql)
            return true if res.num_rows > 0
            false
        end
    end
end
