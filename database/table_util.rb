module Database

    class TableUtil
        
       def self.list_tables db = nil
          db = db || Mysql.default 
          arr = []
          db.connect do
              arr = db.con.list_tables
          end
          return arr
       end
       def self.list_fields table,db = nil
           return [] unless table
           db = db || Mysql.default
           arr = []
           db.connect do 
               arr = db.query("SELECT * FROM #{table} LIMIT 1").fetch_fields
           end
           return arr.map {|f| f.name.to_sym }
       end
              
        def self.add_field table,name, type
            sql = "ALTER TABLE #{table} ADD COLUMN #{name} #{type}"
            db = Mysql.default
            db.connect do 
                db.query(sql)
            end
        end
        def self.rename_table old_name,new_name
            sql = "RENAME TABLE #{old_name} TO #{new_name}"
            db = Mysql.default
            db.connect do 
                db.query(sql)
            end
            Logger.<<(__FILE__,"INFO","Renamed table #{old_name} => #{new_name}")
        end
        def self.reset_table table
            sql = "DELETE FROM #{table};"
            db = Database::Mysql.default
            db.connect do 
                db.query(sql)
            end
            Logger.<<(__FILE__,"INFO","Reset table #{table}")
        end
        def self.delete_table table
            sql = "DROP TABLE  IF EXISTS #{table};"
            db = Database::Mysql.default
            db.connect do 
                db.query(sql)
            end
            Logger.<<(__FILE__,"INFO","Deleted table #{table}")
        end

        def self.compress_table table
            puts "TODO"
        end
        def self.change_engine table,engine,opts = {}
            sql = "ALTER TABLE #{table} ENGINE=#{engine}"
            db = database::Mysql.default
            db.connect { db.query(sql)}
        end

        def self.optimize_table table
            sql = "OPTIMIZE TABLE #{table}"
            db = database::Mysql.default
            db.connect { db.query(sql) }
        end

        def self.reset_file_entries table, entries
            sql = "DELETE FROM #{table} WHERE file_name IN "
            sql << "(" << entries.map{|f| "'#{f}'"}.join(',') << ");"
            db = Database::Mysql.default
            db.connect do
                db.query(sql)
            end
        end

        # return an array of table names  associated with this prefix 
        def self.search_tables prefix_name
            sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                "  WHERE TABLE_NAME REGEXP '^#{prefix_name}(_[0-9]{8})?$';" 
            db = Mysql.default
            names = []
            db.connect do
                res = db.query(sql)
                res.each_hash do |row|
                    names << row['TABLE_NAME']
                end
            end
            names
        end 
    end
end
