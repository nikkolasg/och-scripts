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
                            TableUtil::delete_table @table_records 
                            TableUtil::delete_table @table_records_union 
                            TableUtil::search_tables(@table_records).each do |t|

                                TableUtil::delete_table t
                            end
                        end
                        Logger.<<(__FILE__,"INFO","Deleted #{type} tables for #{@source.name}")
                    end
                end

                def reset type
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
                def insert_files files
                    sql = ""
                    if files.is_a?(Array)
                        sql = "INSERT INTO #{@table_files} (file_name) VALUES "
                        sql += files.map{|f|"('#{f.name}')"}.join(',')
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
                        puts sql if @opts[:d]
                        res = @db.query(sql)
                        ## sends back the number of rows to the caller 
                        ## so it can update progression in real time
                        opts[:proc].call(res.num_rows) if opts[:proc]
                        Logger.<<(__FILE__,"INFO","Retrieved #{res.num_rows} records to be analyzed from #{@source.name} ...")
                        res.each_hash do |row|
                            yield RubyUtil::symbolize(row)
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
                    sql = opts[:union] ? "#{@table_records_union} AS r " : "#{@table_records} AS r "
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
