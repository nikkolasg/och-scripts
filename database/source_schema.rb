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

    ## this module represents all the schema used in the tool
    #they can be used by sources or monitor 
    #so we have one module for each usage       
    module Schema
        require_relative 'sql'
        require_relative '../debugger'

        def set_options opts = {}
            @opts.merge! opts
        end
        ## litlle utility so we can pass an already connected db
        #to this class instead of reconnecting every time
        def set_db db
            @db = db
        end

        ## this module regroups all differents
        #schemas for the sources. It is one schema / source
        # You can specify your own schema class here !
        # it must reponds to 
        #  - create
        #  - delete
        #  - reset
        #  - filter_files files
        #  - insert_files files
        #  - select_new_records with opts ==> YIELD for each record
        #  - insert_new_records (json)
        #
        module Source 

            def self.create klass_name,source, opts = {}
                if klass_name.is_a?(String)
                    klass_name = klass_name.capitalize.to_sym
                end
                Source::const_get(klass_name).new(source,opts)
            end
            class GenericSchema
                include Schema
                attr_reader :table_records,:table_files,:table_records_union
                attr_reader :opts
                def initialize source, opts = {}
                    @source = source
                    @table_files = "FILES_#{source.name.upcase}"
                    @table_records = "RECORDS_#{source.name.upcase}"
                    @table_records_union = "RECORDS_#{source.name.upcase}_UNION"
                    @db = Database::Mysql.default
                    @opts = opts
                    ## by default fetch all results
                    @sql_fetch_all = !(@opts[:sql_no_fetch_all] || false)
                end

                def rename new_name
                    nfiles = "FILES_#{new_name.upcase}"
                    TableUtil::rename_table @table_files,nfiles
                    Logger.<<(__FILE__,"INFO","Renamed files table #{@table_files} to #{nfiles}")
                    @table_files = nfiles

                    nrecords = "RECORDS_#{new_name.upcase}"
                    nrecords_union = "RECORDS_#{new_name.upcase}_UNION"
                    at_records = TableUtil::search_tables(@table_records)
                    ntables = []
                    at_records.each do |table|
                        ntable = table.gsub(@table_records,nrecords)
                        ntables << ntable
                        TableUtil::rename_table table,ntable
                    end
                    ## delete union
                    TableUtil::delete_table @table_records_union
                    Logger.<<(__FILE__,"INFO","Renamed records table #{@table_records} to #{nrecords}")
                    ## affect rights values
                    @table_records = nrecords
                    @table_records_union = nrecords_union
                    ## create again the union
                    sql = SqlGenerator.for_records_union(@table_records_union,
                                                         @source.records_fields,
                                                         union: ntables)
                    @db.connect { @db.query(sql) }

                end

                ## create a table of the specified type
                #type can be :files, or :records
                def create type
                    sql = ""
                    @db.connect do 
                        sql = SqlGenerator.for_files(@table_files,@source.file_length) if type == :files
                        if type == :records
                            sql = SqlGenerator.for_records(@table_records,@source.records_fields) 
                            @db.query(sql)
                            sql = SqlGenerator.for_records_union(@table_records_union,@source.records_fields, union: TableUtil::search_tables(@table_records))
                        end
                        puts sql 
                        @db.query(sql)
                        Logger.<<(__FILE__,"INFO","Setup #{type} tables for #{@source.name}")
                    end
                end

                def delete type
                    @db.connect do 
                        TableUtil::delete_table @table_files if type == :files
                        if type == :records
                            if @opts[:min_date] || @opts[:max_date]
                                delete_filter
                            else
                                TableUtil::delete_table @table_records,@db
                                TableUtil::delete_table @table_records_union,@db 
                                TableUtil::search_tables(@table_records).each do |t|
                                    TableUtil::delete_table t
                                end
                            end
                        end
                        Logger.<<(__FILE__,"INFO","Deleted #{type} tables for #{@source.name}")
                    end
                end

                def reset type
                    if @opts[:min_date] || @opts[:max_date]
                        reset_filter type
                    else
                        @db.connect do
                            TableUtil::reset_table @table_files if type == :files
                            if type == :records 
                                sql = "SELECT distinct file_id from #{@table_records_union}";
                                res = @db.query(sql)
                                ids = []
                                res.each_hash do |row|
                                    ids << row['file_id']
                                end
                                TableUtil::reset_table @table_records if type == :records
                                TableUtil::reset_table @table_records_union if type == :records
                                sql = "UPDATE #{@table_files} SET processed = 0 WHERE file_id IN (#{RubyUtil::sqlize(ids,:no_parenthesis => true, :no_quotes => true)})"
                                @db.query(sql) unless ids.empty?
                            end
                            Logger.<<(__FILE__,"INFO","Reset #{type} tables for #{@source.name}")
                        end
                    end
                end

                ## Reset a table with a time filter
                ### looks for RECORDS between min-date and maxdate
                ## looks for FILES with timestamp between min-date & max-date
                def reset_filter type
                    ## transform date into timestamp
                    min_date = @opts[:min_date] || '02/01/1970' ## if no min date, delete 
                    ## all before max_date then 
                    min_date = Util.date(min_date,format: "%Y-%m-%d %H:%M:%S") 
                    max_date = @opts[:max_date] || 'now'
                    max_date = Util.date(max_date,format: "%Y-%m-%d %H:%M:%S")
                    min_ts = Util.date(min_date,format: "%s")
                    max_ts = Util.date(max_date,format: "%s")
                    ## create & query sql§
                    if type == :files
                        sql = "DELETE FROM #{@table_files} WHERE timest BETWEEN " +
                            " '#{min_date}' AND  '#{max_date}'"
                        puts sql if @opts[:v]
                        @db.connect { @db.query(sql) }
                    elsif type == :records
                        time_field = @source.flow.time_field_records
                        sql_id = "SELECT distinct file_id from #{@table_records_union}" +
                            " WHERE #{time_field} BETWEEN #{min_ts} AND #{max_ts}"
                        @db.connect do
                            puts sql_id if @opts[:v]
                            res = @db.query(sql_id)
                            ids = []
                            ## take the ids that we will remove, so we can mark them as unprocesse
                            res.each_hash { |row| ids << row['file_id'] }
                            sql = "DELETE FROM %s WHERE #{@source.flow.time_field_records} " +
                                "BETWEEN #{min_ts} AND #{max_ts}"
                            sql_unproc = "UPDATE #{@table_files} SET processed = 0 WHERE file_id IN (#{RubyUtil::sqlize(ids,:no_parenthesis => true, :no_quotes => true)})"
                            @db.query(sql % @table_records)
                            @db.query(sql % @table_records_union)
                            @db.query(sql_unproc) unless ids.empty?
                        end
                    end
                end

                ## Will delete table according to some time criterions
                ## Delete RECORDS_{TIME} if time between mindate & maxdate
                ## change union table after.
                ## no file delete ...
                def delete_filter 
                    ## First list all potentials tables
                    tables =  TableUtil::search_tables(@table_records)
                    tables.delete @table_records ## do not delete the current one !
                    ## Then select the one with date in the interval
                    min_date = Util.date(@opts[:min_date] || "02/01/1970")
                    max_date = Util.date(@opts[:max_date] || "now")
                    delete,keep = tables.partition do |name|
                        name =~ /_(\d{8})$/
                        date = $1
                        val = date >= min_date && date <= max_date
                        Logger.<<(__FILE__,"INFO","Will delete table #{name} from #{@source.name}") if val
                        next val
                    end
                    ## need to include it in union statement 
                    keep += [@table_records]
                    ## change the UNION table to only keep the right one
                    sql = "ALTER TABLE #{@table_records_union} " +
                        " UNION=(#{keep.map{|x| "`#{x}`"}.join(',')})"
                    @db.connect do 
                        ## delete table
                        delete.each { |name| TableUtil::delete_table(name,@db) }
                        ## change union
                        puts sql if @opts[:d]
                        @db.query(sql)
                    end
                end

                ## select files unprocessed from the files table of this source
                ## for the GET PART
                def select_new_files
                    res = []
                    @db.connect do 
                        sql = "SELECT * FROM #{@table_files} where processed = 0"
                        res = @db.query(sql)
                    end
                    h = []
                    res.each_hash { |row| h << RubyUtil::symbolize(row) }
                    return h
                end

                ## Modify the files list so to have only unregistered
                #files in the list (i.e. not present in db):
                # for the GET PART
                def filter_files files
                    return files if files.empty?
                    count = files.size
                    sql = "SELECT file_name FROM #{@table_files} WHERE file_name IN "
                    sql += RubyUtil::sqlize(files.map{|f|f.name})
                    res = nil
                    @db.connect do 
                        res = @db.query(sql)
                    end
                    return [] unless res
                    files2rm = []
                    res.each_hash do |row|
                        files2rm << CDR::File.new(row['file_name'],nosearch: true)
                    end
                    return files2rm
                end

                ## insert the files in the db for this schema
                ## Either take a flat list of files (CDR::File obj) OR
                #can take a hash liek this:
                #KEY : Folder
                #VALUE : List of CDR::FILE 
                #for the GET PART
                def insert_files files, folder = nil
                    sql = ""
                    if files.is_a?(Array) && folder 
                        sql = "INSERT INTO #{@table_files} (file_name,folder) VALUES "
                        sql += files.map{|f|"('#{f.name}','#{folder}')"}.join(',')
                        sql += " ON DUPLICATE KEY UPDATE file_name=file_name;"
                    elsif files.is_a?(Hash)
                        sql = "INSERT INTO #{@table_files} (folder,file_name) VALUES "
                        sql += files.map  do |fold,lfiles|
                            lfiles.map { |f| "('#{fold}','#{f.name}')" }
                        end.flatten(1).join(',')
                    end
                    @db.connect do
                        @db.query(sql)
                    end
                end

                ## Update records in the files table to set them as processed
                ## INSERT PART
                def processed_files files_id
                    sql = "UPDATE #{@table_files} SET processed = 1 " +
                        "WHERE file_id IN (" +
                        RubyUtil::sqlize(files_id,no_parenthesis: true) +
                        ");"
                    @db.connect do
                        @db.query(sql)
                    end
                    if @opts[:insert_only]
                        Logger.<<(__FILE__,"INFO","Maked files as inserted in monitors for source #{@source.name}")
                        @source.flow.monitors.each do |mon|
                            mon.schema.processed_files(@source,files_id)
                        end
                    end
                end


                ## set all the files to be unprocessed
                def reset_files files_id = nil
                    sql = "UPDATE #{@table_files} SET processed = 0 " 
                    sql += "WHERE file_id IN (#{RubyUtil::sqlize(files_id,no_parenthesis: true)})" if files_id
                    @db.connect do
                        @db.query(sql)
                    end
                end


                ## file is the hash return from the files table 
                #records is the json output of the decoder
                ## INSERT PART
                def insert_records file_id, records
                    sql = "INSERT INTO #{@table_records} (file_id," 
                    @db.connect do 
                        records.each do |name,hash|
                            fields = hash[:fields]
                            values = hash[:values]
                            next if values.empty?
                            sql_ = sql + fields.keys.join(',') + ") VALUES "
                            sql_ += values.map do |rec| 
                                row = [file_id] + rec.values_at(*fields.values)
                                RubyUtil::sqlize(row)
                            end.join(',')
                            @db.query(sql_)
                        end
                    end
                end



                ## Select all new records to be analzed for a monitor
                # You can also specify a Proc which will be called with
                # a value equal to the number of rows taht will be fetched
                # Used to see progression 
                # PROCESS PART
                def new_records monitor, opts = {}
                    sql = "SELECT " + new_records_select(monitor) +
                        "\nFROM " + new_records_from(monitor,opts) +
                        "\nWHERE " + new_records_where(monitor) +
                        ";"
                    @db.connect do
                        #require 'ruby-debug'
                        puts sql if @opts[:d]
                        ## special case for union where we might have
                        # hundreds of millions of record taht can't fit
                        # into memory
                        if @opts[:union] || !@sql_fetch_all
                            @db.con.query_with_result = false
                            @db.query(sql)
                            res = @db.con.use_result
                        else
                            res = @db.query(sql)
                            ## sends back the number of rows to the caller 
                            ## so it can update progression in real time
                            opts[:proc].call(res.num_rows) if opts[:proc]
                            Logger.<<(__FILE__,"INFO","Retrieved #{res.num_rows} records to be analyzed from #{@source.name} ...")
                        end
                        res.each_hash do |row|
                            yield RubyUtil::symbolize(row)
                        end

                        #debugger if @opts[:d]
                        if @opts[:union] || !@sql_fetch_all
                            res.free
                            @db.con.query_with_result = true
                        end
                    end
                end


                def delete_records_from_fileid fileid
                    @db.connect do
                        sql = "DELETE FROM #{@table_records} WHERE file_id "
                        if fileid.respond_to?(:join)
                            sql += "IN (#{fileid.join(',')})"
                        else
                            sql += " = #{fileid}"
                        end
                        @db.query(sql)
                    end
                end

                protected 

                ## Theses methods return the portion of the query
                #to be computed to get the new records to analyze
                #for this monitor   
                # We agree that we name the records table "r"
                #                       the files table "f"
                # so we can do r.file_id, etc    
                # needed when JOIN is necessary (to get folder name etc)
                def new_records_select monitor
                    time = monitor.time_field || monitor.flow.time_field_records
                    sql = ["r.file_id","r.#{time}"]
                    if monitor.filter ## no necessary filter on a monitor
                        fields = monitor.filter.fields_allowed.dup
                        ## in case we request the folder attribute,
                        #we have to get it back from the files table, so 
                        #prefix it with "f"
                        if (i = fields.index(:folder))
                            fields.delete_at(i)
                            sql << "f.folder"
                        end
                        sql += fields.map { |f| "r.#{f}" }
                    end
                    sql += monitor.stats.fields.map { |f| "r.#{f}" }
                    sql.uniq.join(',')
                end

                def new_records_from monitor,opts = {}
                    table = @opts[:table] || (@opts[:union] ? "#{@table_records_union}" : nil) || "#{@table_records}"
                    table = table + " AS r "
                    sql = table
                    if monitor.filter && monitor.filter.fields_allowed.include?(:folder)
                        sql += "LEFT JOIN #{table_files} AS f " +
                            "ON r.file_id = f.file_id "
                    end
                    return sql
                end

                def new_records_where monitor
                    sql = "r.file_id NOT IN " +
                        "(SELECT file_id FROM " +
                        monitor.schema.table_records(@source) +
                        ")"
                    return sql
                end
            end
        end
    end

end
