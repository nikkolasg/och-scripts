module Database

    module Schema
        require_relative 'sql'

        module Source
            require_relative 'source_schema'
            ## Class that can handle a records table with some fields
            #indexed in a auxiliary table .
            #ax table will be named #{source}_aux_#{field}
            #default is "VARCHAR(50)" for the value
            class IndexerSchema < GenericSchema

                def initialize source, opts = {}
                    super(source,opts)
                    raise "No index fields given to IndexerSchema. Abort." unless @opts[:index]
                    if @opts[:index].is_a?(String)
                        @index_fields = RubyUtil::symbolize(RubyUtil::arrayize(@opts[:index]))
                    elsif @opts[:index].is_a?(Array)
                        @index_fields = RubyUtil::symbolize(@opts[:index])
                    end
                end

                def create type
                    if type == :files 
                        super(type)
                        return
                    end
                    @db.connect do 
                        ## records table
                        ## aux tables
                        @index_fields.each do |field|
                            reg = lambda { |sql| sql.gsub /#{field} [^,]+,/,"#{field} int not null," }
                            records = reg.call(records)
                            records_union = reg.call(records)

                            sql = "CREATE TABLE IF NOT EXISTS " +
                                " #{table_name(field)} (" +
                                " id  int not null auto_increment primary key, " +
                                " value VARCHAR(50) unique ) " +
                                "ENGINE=MyISAM  " 
                            sql = SqlGenerator.append_directories sql
                            @db.query(sql)
                            Logger.<<(__FILE__,"INFO","Created auxiliary table #{field}.")
                        end
                        ## first create the record table
                        records = SqlGenerator.for_records(@table_records,@source.records_fields)
                        @db.query records
                        ## then lookup similar tables
                        records_union = SqlGenerator.for_records_union(@table_records_union,@source.records_fields,union: TableUtil::search_tables(@table_records)) 
                        @db.query records_union
                        Logger.<<(__FILE__,"INFO","Created Records table ... ")
                    end
                end

                def delete type = nil
                    super(type)
                    @db.connect 
                    @index_fields.each do |field|
                        sql = "DROP TABLE IF EXISTS #{table_name(field)} "
                        @db.query(sql)
                        Logger.<<(__FILE__,"INFO","Deleted aux table for #{field}...")
                    end

                end

                def reset type = nil
                    super(type)
                    @db.connect do
                        @index_fields.each do |field|
                            sql = "DELETE FROM #{table_name(field)}"
                            @db.query sql
                            Logger.<<(__FILE__,"INFO","Reset aux table for #{field}")
                        end
                    end
                end
                ## transform some value in the record if thexy need to be indexed 
                def index_value fields,record
                    @index_value ||= get_index_values
                    fields.each do |f,i|
                        ## fast comparison due to use of Symbol and not string
                        next unless @index_fields.include? f
                        if id = @index_value[record[i]]
                            record[i] = id
                        else
                            id =  insert_and_id f,record[i]
                            @index_value[record[i]] = id
                            record[i] = id
                        end
                    end
                end

                ## just override the default behavior to get index too
                def insert_records file_id,records
                    sql = "INSERT INTO #{@table_records} (file_id," 
                    @db.connect do 
                        records.each do |name,hash|
                            fields = hash[:fields]
                            values = hash[:values]
                            sql_ = sql + fields.keys.join(',') + ") VALUES "
                            sql_ += values.map do |rec| 
                                index_value fields,rec ## THIS LINE ;)
                                row = [file_id] + rec.values_at(*fields.values)
                                RubyUtil::sqlize(row)
                            end.join(',')
                            @db.query(sql_)
                        end
                    end
                end

                protected

                def new_records_select monitor
                    time = monitor.time_field || monitor.flow.time_field_records
                    sql = ["file_id","#{time}"]
                    if monitor.filter
                        sql += monitor.filter.fields.dup
                    end
                    sql += monitor.stats.fields.dup
                    ## select the name of the table we take
                    sql.map do |f|
                        if @index_fields.include? f
                            table_name(f) +".value as #{f}"
                        elsif f == :folder
                            "f.folder"
                        else
                            "r.#{f}"
                        end

                    end
                    sql.uniq.join(',')
                end

                def new_records_from monitor,opts = {}
                    sql = opts[:union] ? "#{@table_records_union} AS r " : "#{@table_records} AS r "
                    if monitor.filter 
                        monitor.filter.fields_allowed.each do |f|
                            if f == :folder
                                sql += "LEFT JOIN #{table_files} AS f " +
                                    "ON r.file_id = f.file_id "
                            elsif @index_fields.include? f
                                sql += "LEFT OUTER JOIN #{table_name(f)} ON " +
                                    " r.#{f} = #{table_name(f)}.id "
                            end
                        end
                    end
                    return sql
                end


                ## insert a new aux value and return the ID
                #of the row
                def insert_and_id field, value
                    id = -1
                    sql = "INSERT INTO #{table_name(field)}(value) VALUES " +
                        "(?)"
                    @db.connect do
                        st = @db.prepared_stmt sql
                        st.execute(value)
                        sql = "select last_insert_id()"
                        id = @db.query(sql).fetch_row[0].to_i
                    end
                    return id
                end

                ## return an hash with
                ## key : record value ( string )
                ## value : index in aux table
                def get_index_values
                    hash = {}
                    @db.connect
                    @index_fields.each do |field|
                        tname = table_name field
                        sql = "SELECT id,value FROM #{tname}"
                        res = @db.query(sql)
                        res.each_hash do |row|
                            hash[row['value']] = row['id']
                        end
                    end
                    @db.close 
                    hash
                end

                def table_name field
                    "#{@source.name.to_s.upcase}_AUX_#{field.upcase}"
                end
            end

        end

    end

end
