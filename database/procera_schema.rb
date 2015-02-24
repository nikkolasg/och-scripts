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

    module Schema
        require_relative 'sql'

        module Source

            ## Create a schema that will select new records from
            #the UDR db on ubu18. Theses records are already decoded 
            #and well organized, so what we just have to do is only
            #records the ids we have already processed.
            #It has the capacity to select records by date also
            #like in backlog processing. --min-date="    "
            #                            --max-date="    "
            class ProceraSchema
                
                def initialize source, opts = {}
                    @source = source
                    @opts = opts
                    @host = source.host
                    @db = Database::Mysql.new(host.address,host.database,
                                               host.login,
                                               host.password)
                    @table_records = "RECORDS_PROCERA"
                    @indexFields = [:protocol_category,:protocol_name,:ap_name]
                end
                    
                def create opts = {}
                     sql = "CREATE TABLE #{@table_records} ( " + 
                           "row_id INT UNSIGNED NOT NULL UNIQUE ) " +
                           " ENGINE=MyISAM"
                     sql = Database::SqlGenerator.append_directories(sql)
                     @db.connect do 
                         @db.query(sql)
                         indexFields.each do |f|
                            tableName = MAPPING[f]
                            create_aux_table(tableName) unless TableUtil::table_exists?(tableName,@db)
                         end
                     end
                     Logger.<<(__FILE__,"INFO","Created Procera records table.")
                end

                def reset opts = {}
                    sql = " DELETE FROM #{ @table_records} "
                    @db.connect do
                        @db.query(sql)
                    end
                    Logger.<<(__FILE__,"INFO","Reseted Procera records table")
                end

                def delete opts={}
                    sql = "DROP TABLE #{@table_records}"
                    @db.connect { 
                        @db.query(sql) 
                        @indexFields.each do |f|
                            tableName = MAPPING[f]
                            @db.query "DROP TABLE #{tableName}"
                        end
                    }
                    Logger.<<(__FILE__,"INFO","Deleted Procera records table");
                end
                
                def index_value fields,record
                    @db.connect do 
                    fields.each do |f|
                        next unless (tableName = MAPPING[f])
                        id = insert_value tableName,record[f]
                        record[f] = id
                    end
                    end
                end


                def insert_value tableName,value
                    sql = "insert into #{tableName}(value) VALUES (#{value}) on duplicate key update id=id"
                    @db.connect { @db.query(sql) }
                end
                ## create the index table
                def create_aux_table table_name
                    sql = "CREATE TABLE #{table_name} ( " +
                        " id int not null auto_increment primary key, " +
                        " value VARCHAR(50) DEFAULT '') " +
                    @db.connect { @db.query(sql) }

                end
               

                MAPPING = {
                    :protocol_category  => :listProtocolCategory,
                    :protocol_name      => :listProtocolName,
                    :device_type        => :listDeviceType,
                    :apn                => :listApn,
                    :browser            => :listBrowser,
                    :os                 => :listOs

                } 
            end

        end

    end

end