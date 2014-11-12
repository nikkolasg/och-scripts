module Stats
    require './process/backlog.rb'
    def self.create type, opts = {}
        case type
        when :generic
            return GenericStats.new opts[:flow],opts
        when :backlog
            type = (opts[:monitor] ? :monitor : nil) || (opts[:flow] ? :flow : nil)
            return BacklogStats.create type,opts
        end
    end

    # format the time according to the monitor specification
    def get_formatted_time record_time

        interval = @current.time_interval

        # compute the "bin number" from the Epoch, 
        # i.e. the smallest integer m such taht the time falls into 
        # [m, m+ interval]
        # return the date from it
        y,m,d,h,mn,sec = Util::decompose record_time 
        time_mn = Time.at(Time.utc(y,m,d,h,mn)).to_i / Util::SEC_IN_MIN
        minimum = time_mn - (time_mn % interval) 
        formatted_time = Time.at(minimum * Util::SEC_IN_MIN).to_i
    end
    ## format the  stats of the monitors in its table
    #it organize like the table structure
    #with a array of rows and an array of fields
    def format_stats 
        hash = @monitors_stats[@current.name]
        # columns      COL1 => 0 , COL2 => 1 ..]
        # datas       [ [ v11,v12,v13...]  ,i.e. one row of the db
        #               [ v21.v22...    ]
        #             ]            columns = { App.database.timestamp => 0 }
        columns = { App.database.timestamp => 0 }
        @current.filter_records.map {|f| [@current.column_record(f,:input),@current.column_record(f,:output)] }.flatten(1).each_with_index { |v,i| columns[v] = i+1 }
        datas = []
        hash.each do |time,type_hash|
            values = Array.new(columns.size,0)
            values[0] = time
            type_hash.each do |type,dir_hash|
                # put the value at right offset in the array "values"
                # for this type and this dir
                dir_hash.each do |dir, number|
                    column_index = columns[@current.column_record(type,dir)]
                    values[column_index] = number
                end
            end
            datas << values
            Logger.<<(__FILE__,"DEBUG", "#{time} : #{hash[time].inspect}")
        end
        timelines = hash.keys.size
        Logger.<<(__FILE__,"INFO","Monitor #{@current.name} has recorded #{timelines} different time lines ")
        insert_stats columns,datas.uniq
    end
    
    ## actually insert the formatted data in the DB
    def insert_stats columns,stats
        return if stats.empty? 
        table =  @current.table_stats 
        sql = "INSERT INTO #{table} ( "
        sql += columns.keys.join(',') + ")"
        sql += " VALUES "
        sql += stats.map { |row|  RubyUtil::sqlize row,no_quote: true }.join(',')
        # add the resulsts to previously inserted row !
        sql += " ON DUPLICATE KEY UPDATE "

        sql += columns.keys[1..-1].map { |col| "#{col}=#{col} + VALUES(#{col})"}.join(',')
        sql += ";"
        Logger.<<(__FILE__,"DEBUG",sql)
        @db.query sql
    end
    
    # mark the files as processed
    # in the records db for the current monitor
    def mark_as_processed opts = {} 
        count = 0
        going = opts[:dir] || :both
        Util::starts_for (going) do |dir|
            table = opts[:table] || @current.table_records(dir)
            sql = "INSERT INTO #{table} (file_id) VALUES "
            list_ids = @monitors_processed[@current.name][dir]
            next if list_ids.empty?
            sql +="#{ list_ids.map { |id| "(" + id + ")" }.join(',') };"
            Logger.<<(__FILE__,"DEBUG",sql)
            @db.query(sql)
            count += list_ids.size
        end
        Logger.<<(__FILE__,"INFO","Monitor #{@current.name} has processed #{count} files !")
    end

    # just print the progression,
    # since we can not print every records(too much) we have to
    # print like every ten percetn or something
    def progression counter
        @steps ||= (1..10).map { |n| [(@num_rows.to_f / 10.to_f) * n,n*10] }
        return if @steps.empty?
        c,perc = @steps[0] 
        if counter > c
            Logger.<<(__FILE__,"INFO","Having processed #{perc}%  now ...")
            @steps.shift
        end
    end

    class GenericStats
        require 'set'
        attr_accessor :current
        include Stats
        def initialize flow,opts
            @opts = opts
            @flow = flow
            @monitors = opts[:monitor] ? RubyUtil::arrayize(opts[:monitor]) : @flow.monitors
            @current = nil # current monitor being analysed
            # Cache proxy that stores the results of the filters
            # for each monitors for each value
            @monitors_filters_done = Hash.new { |h,k| h[k] = {} }
            ## represent the stats itself (number of records with bla bla bla
            # organized by
            # Monitor => Time(formatted) => Type of record => direction => number =)
            @monitors_stats = Hash.new do |hash,key|
                dir = { input: 0 , output: 0 }
                sub_hash = Hash.new { |h,k| h[k] = dir.clone }
                # monitor => time => type => dir => count
                hash[key] = Hash.new { |h,k| h[k] = sub_hash.clone }
            end
            ## represent all the IDs that monitors have processed
            @monitors_processed = Hash.new do |h,k|
                h[k] = { input: Set.new, output: Set.new }
            end
        end

        def compute
            counter = 0.0
            new_records do |record,dir|
                progression counter
                counter += 1
                @monitors.each do |m|
                    @current = m
                    analyse_record record,dir
                end
                Logger.<<(__FILE__,"INFO","Record #{record[:id]}:#{record[:name]} has been analyzed ...") if @opts[:v]
            end
            # only insert after 
            @db = Database::Mysql.default
            @db.connect do 
                @monitors.each do |m| 
                    @current = m
                    format_stats
                    mark_as_processed
                end
            end
        end
        # analyse a single record for a single monitor
        def analyse_record record,dir
            return unless allow? record
            hash = @monitors_stats[@current.name]
            record_time = get_time_from_record record
            time = get_formatted_time record_time
            ## WOW ONE LINE STATS ! :D
            hash[time][record[:name]][dir] += 1
            ## this id has been processed
            @monitors_processed[@current.name][dir] << record[:file_id]
        end


        ## is the record will be analyzed by this monitor ?
        def allow? record

            # check if this records belongs to this monitor
            return false unless @current.switches.include? record[:switch]
            # check if the record type belongs to this monitor
            return false if @current.filter_records unless @current.filter_records.include? record[:name]

            # check if all filters return true !
            @current.filters.each do |field,block|
                # check if we have results cached
                if @monitors_filters_done[@current.name].key? field 
                    # this filter let pass , so we check next on
                    if @monitors_filters_done[@current.name][field] == true
                        next
                    else # this filter does not pass so directly say no !
                        return false
                    end
                end

                # caching results
                out = block.call(record[field])
                @monitors_filters_done[@current.name][field] = out
                if out == true
                    next
                else
                    return false
                end
            end
            return true

        end


        private
        def get_time_from_record record
            field_time = @flow.time_field_records.downcase.to_sym
            record[field_time]
        end

        # retrieve new records that may be used by all monitors
        # so DB access is limited
        # => union of output ,input sources
        def new_records
            extra_clauses = []
            extra_clauses << extra_clause_on(:switches)
            extra_clauses << extra_clause_on(:records)
            extra_clauses.compact!

            extra_select = [] 
            extra_select << "id"
            extra_select << "file_id"
            extra_select << "switch"
            extra_select << "name"
            extra_select << @flow.time_field_records
            extra_select << extra_clause_on(:fields)
            extra_select.compact!

            sql = "SELECT "
            unless extra_select.empty?
                sql += extra_select.join(' , ')
            end

            db = Database::Mysql.default
            db.connect do

                [:input,:output].each do |dir|
                    sql_ = sql.clone
                    table = @flow.table_records(dir)

                    sql_ += " FROM #{table} "
                    ## REMOVE the row when in the MONITORS table
                    mon_tables = @monitors.map{|m| " SELECT file_id FROM #{m.table_records(dir)} "}
                    sql_ += " WHERE "
                    sql_ += "file_id NOT IN ( #{mon_tables.join("UNION ALL")} ) "

                    unless extra_clauses.empty?
                        sql_ += " AND "
                        sql_ += extra_clauses.join(' AND ')
                    end
                    sql_ += ";"
                    res = db.query(sql_)
                    @num_rows = res.num_rows
                    res.each_hash do |row|
                        yield RubyUtil::symbolize(row), dir if block_given?
                    end
                end
            end
        end

        ## compute somes rules to custom the query to the db
        # so we need limited access
        def extra_clause_on on
            case on
            when :switches
                switches = @monitors.map { |m| m.switches }.flatten(1).uniq.sort
                unless @flow.switches.sort == switches # Youra, we just take all records
                    ## we gotta to add a clause :(
                    return "switch IN #{RubyUtil::sqlize switches}"         
                end
            when :records
                records2keep = @monitors.map { |m| m.filter_records }.flatten(1).uniq.sort
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

