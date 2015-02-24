#
# Copyright (C) 2014-2015 Nicolas GAILLY for Orange Communications SA, Switzerland
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
module Database

    class TableUtil
       
        def self.table_exists? tableName, db = nil
            sql = "SHOW TABLES LIKE '#{tableName}'"
            db = db || Mysql.default
            arr = []
            db.connect do 
               arr = db.query(sql)
            end
            arr.num_rows > 0 ? true : false
        end

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
        def self.rename_table old_name,new_name,db = nil
            sql = "RENAME TABLE #{old_name} TO #{new_name}"
            db = db || Mysql.default
            db.connect do 
                db.query(sql)
            end
            Logger.<<(__FILE__,"INFO","Renamed table #{old_name} => #{new_name}")
        end
        def self.reset_table table,db = nil
            sql = "DELETE FROM #{table};"
            db = db || Database::Mysql.default
            db.connect do 
                db.query(sql)
            end
            Logger.<<(__FILE__,"INFO","Reset table #{table}")
        end
        def self.delete_table table,db = nil
            sql = "DROP TABLE  IF EXISTS #{table};"
            db = db || Database::Mysql.default
            db.connect do 
                db.query(sql)
            end
            Logger.<<(__FILE__,"INFO","Deleted table #{table}")
        end

        def self.compress_table table,db = nil
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
        #  and possibly with a date suffix
        def self.search_tables prefix_name,db = nil
            sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                "  WHERE TABLE_NAME REGEXP '^#{prefix_name}(_[0-9]{8})?$';" 
            db = db || Mysql.default
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
