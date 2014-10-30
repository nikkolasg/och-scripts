module Stats

    def self.create type, opts = {}
        GenericStats.new type,opts
    end


    class GenericStats
        attr_accessor :current
        def initialize flow_name,opts
            @flow = App.flow(flow_name)
            @monitors = opts[:monitor] ? @flow.monitors(opts[:monitor]) : @flow.monitors
            @current = nil # current monitor being analysed
            @records = {} # records to be analyzed
            @current_filters_done = {} #cache all filters block results in memory 
            #so filtering is faster for the current monitor
            #
            #
            @monitors_filters_done = Hash.new { |h,k| h[k] = {} }
            @monitors_stats = Hash.new do |hash,key|
                col = Hash.new { |h,k| h[k] = 0}            
                dir = { input: col.clone , output: col.clone }
                sub_hash = Hash.new { |h,k| h[k] = dir.clone }
                # monitor => time => type => dir => count
                hash[key] = Hash.new { |h,k| h[k] = sub_hash.clone }
            end

        end

        def compute
            retrieve_new_records 
            Logger.<<(__FILE__,"INFO","YOu retreived a lots of records")
            @monitors.each do |m|
                monitor_single m
            end
            Logger.<<(__FILE__,"INFO","Statistics operations finished !")
        end
        #def compute
            #new_records do |record,dir|
                #@monitors.each do |m|
                    #analyse_record m,record,dir
                #end
            #end
            #@monitors.each { |m| store_stats monitor }
        #end
        ## analyse a single record for a single monitor
        #def analyse_record monitor,record,dir
            #@current = monitor
            #return unless allow? record
            #hash = @monitors_stats[@current]
            #time = get_formatted_time record
            #hash[time][record[:name]][dir] += 1
        #end
        #def store_stats monitor
            #@current = monitor
            #hash = @monitors_stats[monitor]
                        ## columns      COL1 => 0 , COL2 => 1 ..]
            ## datas       [ [ v11,v12,v13...]  ,i.e. one row of the db
            ##               [ v21.v22...    ]
            ##             ]            columns = { App.database.timestamp => 0 }
            #datas = []
            #columns = {App.database.timestamp => 0}
            #monitor.filter_records.map {|f| [monitor.column_record(f,:input),monitor.column_record(f,:output)] }.flatten(1).each_with_index { |v,i| columns[v] = i+1 }

            #hash.each do |time,type|
                #values = [ time ]
                #type.keys.each do |dir|
                    #values[columns[monitor.column_record(type,dir)] = type[dir]
                #end
                #datas << values
            #end
            #insert_stats columns,datas 
        #end

        #def allow? record
            ## check if this records belongs to this monitor
            #return false unless @current.switches.include? record[:switch]
            ## check if the record type belongs to this monitor
            #return false if @current.filter_records unless @current.filter_records.include? record[:name]

            ## check if all filters return true !
            #@current.filters.each do |field,block|
                ## check if we have results cached
                #if @monitors_filters_done[@current].key? field 
                    #if @monitors_filters_done[@current][field] == true
                        #next
                    #else
                        #return false
                    #end
                #end

                ## caching results
                #out = block.call(record[field])
                #@monitors_filters_done[field] = out

                #if out == true
                    #next
                #else
                    #return false
                #end
            #end
            #return true

        #end
        #private
        # do the actual computation for this specific monitor
        def monitor_single monitor
            str = "Compute statistics for monitor #{monitor.name} ..."
            @current = monitor
            switches = (@current.input + @current.output).map do |s| 
                @flow.sources(s).switches
            end.flatten(1)           
            count_kept = 0
            filtered = Hash.new { |h,k| h[k] = { input: [], output: [] } }
            @records.each do |dir,list_records| 
                list_records.each do |record|
                    next unless filtered? record
                    count_kept += 1
                    time = get_formatted_time record
                    filtered[time][dir] << record 
                end
            end
            col,datas  = stats_records filtered
            str += " Filtered #{count_kept} records."
            Logger.<<(__FILE__,"INFO",str)
            insert_stats col,datas
            str += "Inserted for #{filtered.keys.size} rows. "
            Logger.<<(__FILE__,"INFO",str)
            mark_as_processed filtered
        end

        # mark the files as processed
        # in the records db
        def mark_as_processed filtered
            ids = Hash.new { |h,k| h[k] = [] }
            filtered.keys.each do |time|
                [:input,:output].each do |dir|
                ids[dir] += filtered[time][dir].map {|rec| rec[:id] }
                end
            end
            ids.each do  |dir,list_id|
                table = @flow.table_records(dir)
                sql = "UPDATE #{table} SET processed = 1 "
                sql +="WHERE id IN #{ RubyUtil::sqlize list_id, no_quote: true };"
                db = Datalayer::MysqlDatabase.default
                db.connect do
                    db.query(sql)
                end
            end
        end
        
        def insert_stats columns,stats
            return if stats.empty? 
            table = @current.table 
            sql = "INSERT INTO #{table} ( "
            sql += columns.keys.join(',') + ")"
            sql += " VALUES "
            sql += stats.map { |row|  RubyUtil::sqlize row,no_quote: true }.join(',')
            # add the resulsts to previously inserted row !
            sql += " ON DUPLICATE KEY UPDATE "
            
            sql += columns.keys[1..-1].map { |col| "#{col}=#{col} + VALUES(#{col})"}.join(',')
            sql += ";"
            puts sql 
            db = Datalayer::MysqlDatabase.default
            db.connect do 
                db.query sql
            end
        end
        # compute the stats and insert them
        # from the records sorted by the formated time 
        def stats_records records_by_time
            # columns      COL1 => 0 , COL2 => 1 ..]
            # datas       [ [ v11,v12,v13...]  ,i.e. one row of the db
            #               [ v21.v22...    ]
            #             ]            columns = { App.database.timestamp => 0 }
            columns = {App.database.timestamp => 0}
            @current.filter_records.map {|f| [@current.column_record(f,:input),@current.column_record(f,:output)] }.flatten(1).each_with_index { |v,i| columns[v] = i+1 }

            stats = []
            records_by_time.keys.each do |time|
                # prepare one "row" for the db
                values = Array.new(columns.size) { 0 }
                values[0] = time
                [:input,:output].each do |dir|
                    # records at this time for this direction
                    records = records_by_time[time][dir]
                    records.each do |rec|
                        key = @current.column_record rec[:name],dir
                        ind = columns[key]
                        values[ind] += 1
                    end    
                end
                # add the resulst for this time to the global stats
                stats << values
            end
            return columns,stats
        end

        # format the time according to the monitor specification
        def get_formatted_time record

            interval = @current.time_interval
            field_time = @flow.time_field_records.downcase.to_sym
            record_time = record[field_time] # get the time value

            # compute the "bin number" from the Epoch, 
            # i.e. the smallest integer m such taht the time falls into 
            # [m, m+ interval]
            # return the date from it
            y,m,d,h,mn,sec = Util::decompose record_time 
            time_mn = Time.at(Time.utc(y,m,d,h,mn)).to_i / Util::SEC_IN_MIN
            minimum = time_mn - (time_mn % interval) 
            formatted_time = Time.at(minimum * Util::SEC_IN_MIN).to_i
        end

        # return true if this record has to be kept for the current monitor
        # flase otherwise
        def filtered? record
            # check if this records belongs to this monitor
            return false unless @current.switches.include? record[:switch]
            # check if the record type belongs to this monitor
            return false if @current.filter_records unless @current.filter_records.include? record[:name]

            # check if all filters return true !
            @current.filters.each do |field,block|
                # check if we have results cached
                if @current_filters_done.key? field 
                    if @current_filters_done[field] == true
                        next
                    else
                        return false
                    end
                end

                # caching results
                out = block.call(record[field])
                @current_filters_done[field] = out

                if out == true
                    next
                else
                    return false
                end
            end
            return true
        end

        # retrieve new records that may be used by all monitors
        # so DB access is limited
        # => union of output ,input sources
        def retrieve_new_records
            extra_clauses = []
            extra_clauses << "processed = 0" ## easy one
            extra_clauses << extra_clause_on(:switches)
            extra_clauses << extra_clause_on(:records)
            extra_clauses.compact!

            extra_select = [] 
            extra_select << "id"
            extra_select << "switch"
            extra_select << "name"
            extra_select << @flow.time_field_records
            extra_select << extra_clause_on(:fields)
            extra_select.compact!

            sql = "SELECT "
            [:input,:output].each do |dir|
                sql_ = sql.clone
                table = @flow.table_records(dir)
                unless extra_select.empty?
                    sql_ += extra_select.join(' , ')
                end

                sql_ += " FROM #{table} "

                unless extra_clauses.empty?
                    sql_ += "WHERE "
                    sql_ += extra_clauses.join(' AND ')
                end
                sql_ += ";"
                @records[dir] = query_db_with sql_
            end
        end

        # make the actual query and transform in the variables
        # Array of Hashes which are Records
        def query_db_with sql
            records = []
            db = Datalayer::MysqlDatabase.default
            db.connect do
                res = db.query(sql)
                res.each_hash do |row|
                    records << RubyUtil::symbolize(row) 
                end
            end
            records
        end

        ## compute somes rules to custom the query to the db
        # so we need limited access
        def extra_clause_on on
            case on
            when :switches
                sources = @monitors.map{|m| m.inputs + m.outputs}.flatten(1).uniq
                switches = sources.map { |s| @flow.sources(s).switches }.flatten(1).sort.uniq
                unless @flow.switches.sort == switches # Youra, we just take all records
                    ## we gotta to add a clause :(
                    return "switch IN #{RubyUtil::sqlize switches}"         
                end
            when :records
                records2keep = @monitors.map { |m| m.filter_records }.flatten(1).sort.uniq
                records_allowed = @flow.records_allowed.sort
                ## we can just fetch all and looking for specific records
                unless records2keep == records_allowed
                    return "name IN #{RubyUtil::sqlize records2keep}"
                end
            when :fields
                # by construction, we just need to count the records if no
                # filter_field is present, otherwise we take the necessary fields
                takeFields = @monitors.any? { |m| !m.filters.empty? }
                if takeFields
                    return @monitors.map { |m| m.filters.keys }.flatten(1).uniq.join(',') + " "
                end
            end

        end

    end

end
