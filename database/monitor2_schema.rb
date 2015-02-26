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

        module Monitor

            require_relative 'monitor_schema'
            class NewGenericSchema 
                TABLE_SOURCE_INDEX = "SOURCE_INDEX"
                TABLE_TYPE_INDEX = "TYPE_INDEX"
                attr_reader :table_stats
                def initialize monitor,opts ={}
                    @monitor = monitor
                    @db = Mysql.default
                    @table_stats = "MON_#{@monitor.name.upcase}"
                    @opts = opts
                    @opts[:d] = true
                end


                def set_db db
                    @db = db
                end

                def create type
                    @db.connect do
                        if type == :files
                            @monitor.sources.each do |source|
                                table_name = table_records(source)
                                sql= SqlGenerator.for_monitor_source(table_name)
                                @db.query(sql)
                                table_name = table_records(source,backlog:true)
                                flength = source.file_length
                                sql = SqlGenerator.for_monitor_source_backlog(table_name,flength)
                                Logger.<<(__FILE__,"DEBUG",sql) if @opts[:d]
                                @db.query(sql) 
                                Logger.<<(__FILE__,"INFO","Setup files table of #{source.name} for #{@monitor.name}")
                            end
                        elsif type == :stats
                            create_table_source_index unless TableUtil::table_exists?(TABLE_SOURCE_INDEX)
                            create_table_type_index unless TableUtil::table_exists?(TABLE_TYPE_INDEX)
                            sql = " CREATE TABLE IF NOT EXISTS #{@table_stats} ( " +
                                "id int not null  auto_increment primary key," +
                                "timest int not null ," +
                                "source int not null, " +
                                "type int not null, " + 
                                "counter bigint default 0, " + 
                                "unique (timest,source,type) ) " +
                                "ENGINE = MyISAM " 
                            sql = SqlGenerator.append_directories sql  
                            @db.query sql
                        end
                    end
                end
               
                ## insert into db the name of processed files by the backlog process
                def backlog_processed_files source,files
                    return if files.empty?
                    sql = "INSERT INTO " +
                        "#{table_records(source,backlog: true)} " +
                        "(file_name) " +
                        "VALUES " + 
                        files.map{|f| "('"+f.name+"')" }.join(',')
                    @db.connect do
                        @db.query(sql)
                    end
                end


                ## return the name of the files stored in the backlog table
                def backlog_saved_files source
                    l = []
                    @db.connect do 
                        sql = "SELECT file_name FROM #{table_records(source,backlog: true)} "
                        res = @db.query(sql)
                        res.each_hash do |row|
                            l << CDR::File.new(row['file_name'])
                        end
                    end
                    return l
                end


                ## return the table name used by this monitor for this source
                #i.e. the table where the file_id for this source are stored
                def table_records source,opts = {}
                    s = "MON_" + "RECORDS_" +@monitor.name.upcase.to_s + "_" + source.name.upcase.to_s
                    s += "_BCK" if opts[:backlog]
                    return s
                end

                def delete type
                    sql = ""
                    if type == :stats
                        TableUtil::delete_table(@table_stats)
                        Logger.<<(__FILE__,"INFO","Delete stats table #{@table_stats} for #{@monitor.name}")

                    elsif type == :files
                        @monitor.sources.each do |source|
                            table = table_records(source)
                            TableUtil::delete_table(table)
                            table = table_records(source,backlog: true)
                            TableUtil::delete_table(table)
                            Logger.<<(__FILE__,"INFO","Delete files table of #{source.name} for #{@monitor.name}")
                        end
                    end
                end 

                def reset type
                    if type == :stats
                        TableUtil::reset_table(@table_stats)
                        Logger.<<(__FILE__,"INFO","Reset stats table for #{@monitor.name}")

                    elsif type == :files
                        @monitor.sources.each do |source|
                            table = table_records(source)
                            TableUtil::reset_table(table)
                            table = table_records(source,backlog: true)
                            TableUtil::reset_table(table)
                            Logger.<<(__FILE__,"INFO","Reset files table of #{source.name} for #{@monitor.name}")

                        end
                    end
                end

                ## insert the stast relative to a source
                def insert_stats source
                    present = false
                    source_id = get_source_id source
                    what = Set.new
                    stats = @monitor.stats
                    @db.connect do 
                        rows = []
                        stats.summary.each do |ts,hash|
                            hash.each do |type,value|
                                type_id = get_type_id(type)
                                row = [ts,source_id,type_id,value]
                                present ||= true
                                rows << row
                                raise "wow #{row}" unless what.add?([ts,source_id,type_id])

                            end
                        end
                        insert_rows rows if present
                    end
                end

                ## return the id of a source
                def get_source_id source
                    @sources_indexes ||= retrieve_sources_indexes
                    return @sources_indexes[source.name] || set_get_source_id(source)
                end

                ## return the id of a type ( type)
                def get_type_id type
                    @types_indexes ||= retrieve_types_indexes
                    return @types_indexes[type] || set_get_type_id(type)
                end

                ## retrieves all sources / ids stored in db
                def retrieve_sources_indexes
                    retrieve_indexes TABLE_SOURCE_INDEX
                end

                ## if a source is not yet in the table, add it and return the
                #  id for this source
                def set_get_source_id source
                    id = set_get_id TABLE_SOURCE_INDEX,source.name
                    @sources_indexes[source.name] = id
                    id
                end

                ## retrieve the id of the types (type                     
                def retrieve_types_indexes
                    retrieve_indexes TABLE_TYPE_INDEX
                end

                ## if a type  / type is not yet in the table, add it 
                #and return the id
                def set_get_type_id type
                    id = set_get_id TABLE_TYPE_INDEX,type
                    @types_indexes[type] = id
                    id
                end

                ## insert processed files id into right table
                def processed_files source,ids
                    return if ids.empty?
                    sql = "INSERT INTO #{table_records(source)} " +
                        "(file_id) VALUES " + ids.map { |i| "(#{i})"}.join(',') 
                    sql += " ON DUPLICATE KEY UPDATE file_id = file_id;"
                    @db.connect do
                        @db.query(sql)
                    end
                end

                private

                ## insert theses rows into the stats table
                def insert_rows rows
                    sql = "INSERT INTO #{@table_stats}( " +
                        " #{Conf::database.timestamp},source,type,counter) " +
                        " VALUES "  
                    sql += rows.map do |row|
                        RubyUtil::sqlize row
                    end.join(',')
                    sql += " ON DUPLICATE KEY UPDATE " +
                        "counter = VALUES(counter) + counter "
                    @db.connect  do
                        @db.query sql
                    end
                end

                ## set a value and then retreive its id
                def set_get_id table,value
                    sql = "INSERT INTO #{table} (name) " +
                        "VALUES ('#{value}')"
                    id =  0
                    @db.connect do 
                        @db.query(sql)
                        id = @db.query("select last_insert_id()").fetch_row[0].to_i
                    end
                    id
                end

                ## retrieve indexes for a table
                def retrieve_indexes table
                    hash = {}
                    @db.connect do
                        res = @db.query "SELECT id,name from #{table}"
                        res.each_hash do |row|
                            hash[row['name'].strip.to_sym] = row['id']
                        end
                    end
                    hash
                end

                ## create the table used for storing source id
                def create_table_source_index 
                    create_table_index TABLE_SOURCE_INDEX
                    Logger.<<(__FILE__,"INFO","Created table source index for monitors ..")
                end

                ## create the table used for storing type id
                def create_table_type_index
                    create_table_index TABLE_TYPE_INDEX
                    Logger.<<(__FILE__,"INFO","Created table type index for monitors ...")
                end

                ## create a generic table with id / value scheme
                def create_table_index name
                    sql = "CREATE TABLE IF NOT EXISTS #{name} ( " +
                        "id int not null auto_increment primary key, " +
                        "name varchar(25) not null unique) " +
                        "ENGINE= MyISAM " 
                    sql = SqlGenerator.append_directories sql
                    @db.connect do
                        @db.query sql
                    end
                end


            end        
        end

    end

end
