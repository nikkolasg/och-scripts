module Database

    module Schema
        ## Module to handle all schemas relative to the monitors
        # each class inside must respond to 
        #  -create
        #  -delete
        #  -reset
        #  -insert_stats
        #  -insert_files source,files
        module Monitor

            def self.create klass_name, monitor,opts = {}
                if klass_name.is_a?(String)
                    klass_name = klass_name.capitalize.to_sym
                end
                Monitor::const_get(klass_name).new(monitor,opts)
            end


            class GenericSchema
                attr_reader :table_stats
                def initialize monitor,opts = {}
                    @monitor = monitor
                    @db = Mysql.default
                    @table_stats = "MON_#{@monitor.name.upcase}"
                end

                def set_db db
                    @db = db
                end

               def rename_source source, nname
                   name = table_records source
                   bname =table_records source,backlog: true
                   ntname = name.gsub(source.name.upcase.to_s,nname.upcase.to_s)
                   nbtname = bname.gsub(source.name.upcase.to_s,nname.upcase.to_s)
                  TableUtil::rename_table name,ntname
                  TableUtil::rename_table bname,nbtname
                  Logger.<<(__FILE__,"INFO","Renamed records source table for monitor #{@monitor.name}")
               end


                ## return the table name used by this monitor for this source
                #i.e. the table where the file_id for this source are stored
                def table_records source,opts = {}
                    s = "MON_" + "RECORDS_" +@monitor.name.upcase.to_s + "_" + source.name.upcase.to_s
                    s += "_BCK" if opts[:backlog]
                    return s
                end

                ## return all columns name for a source
                def columns_name source
                    @monitor.stats.columns.map { |c| column_name(c,source) }
                end
                ## return one column name for a source
                def column_name stats_col,source
                    return "#{source.name}_#{stats_col}".to_sym
                end

                ## return the columns name of the table for this monitor
                #either for this source
                #or for all sources
                #DO NOT INCLUDE timestamp =)
                def stats_columns source = nil
                    if source ## only for this source
                        return columns_name source
                    else ## all columns
                        @all_columns ||= @monitor.sources.inject([]) do |col,s|
                            col += columns_name s
                            col
                        end
                        return @all_columns
                    end
                end

                ## can accept :stats, or :files as arguments
                def create type
                    sql = ""
                    @db.connect do 
                        if type == :stats
                            cols = @monitor.sources.map { |s| stats_columns(s) }.flatten
                            sql = SqlGenerator.for_monitor_stats(@table_stats,cols)
                           @db.query(sql)
                            Logger.<<(__FILE__,"INFO","Setup stats table for #{@monitor.name}")
                        elsif type == :files
                            @monitor.sources.each do |source|
                                table_name = table_records(source)
                                sql= SqlGenerator.for_monitor_source(table_name)
                                @db.query(sql)
                                table_name = table_records(source,backlog:true)
                                sql = SqlGenerator.for_monitor_source_backlog(table_name)
                                @db.query(sql) 
                                Logger.<<(__FILE__,"INFO","Setup files table of #{source.name} for #{@monitor.name}")
                            end
                        end
                    end
                end

                def delete type
                    sql = ""
                    if type == :stats
                        TableUtil::delete_table(@table_stats)
                        Logger.<<(__FILE__,"INFO","Delete stats table for #{@monitor.name}")

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

                                ## Insert the statistics of this monitor
                def insert_stats source 
                    stats = @monitor.stats
                    @db.connect do
                        ## Timestamp ==> columns
                        stats.summary.each do |ts,hash|
                            row = [[Conf::database.timestamp,ts]]
                            ## Column ==> value
                            hash.each do |column,value|
                                row << [column_name(column,source),value]
                            end
                            insert_row_stats row
                        end
                    end
                end

                ## Mark the files id as proccessed for this source
                def processed_files source, ids
                    return if ids.empty?
                    sql = "INSERT INTO #{table_records(source)} " +
                        "(file_id) VALUES " + ids.map { |i| "(#{i})"}.join(',') 
                    @db.connect do 
                        @db.query(sql)
                    end
                end

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

                private 

                # utilityy used by insert_stats method.
                # Since we dont know if each column is there in every row,
                # preferable to insert row by row 
                def insert_row_stats row
                    return if row.empty?
                    h = Hash[row]
                    sql = "INSERT INTO #{@table_stats} " +
                        RubyUtil::sqlize(h.keys,no_quote:true) +
                        " VALUES " +
                        RubyUtil::sqlize(h.values,no_quote: true) +
                        " ON DUPLICATE KEY UPDATE " +
                        h.keys[1..-1].map {
                            |col| "#{col}=#{col}+VALUES(#{col})"}.join(',')
                        @db.query(sql)
                end



            end
        end
    end
end
