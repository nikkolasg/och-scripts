module Stats
    require_relative 'backlog.rb'
    def self.create type, opts = {}
        case type
        when :generic
            return GenericStats.new opts[:flow],opts
        when :backlog
            return BacklogStats.create opts
        end
    end

    #def get_formatted_time record_time

        #interval = @current.time_interval

        ## compute the "bin number" from the Epoch, 
        ## i.e. the smallest integer m such taht the time falls into 
        ## [m, m+ interval]
        ## return the date from it
        #y,m,d,h,mn,sec = Util::decompose record_time 
        #puts y,m,d,h,mn,sec , " FROM #{record_time}"
        #time_mn = Time.at(Time.utc(y,m,d,h,mn)).to_i / Util::SEC_IN_MIN
        #minimum = time_mn - (time_mn % interval) 
        #formatted_time = Time.at(minimum * Util::SEC_IN_MIN).to_i
    #end

    # format to the monitor interval endpoints
    def get_formatted_time record_time
        interval = @current.time_interval
        time_mn = record_time.to_i / Util::SEC_IN_MIN
        minimum = time_mn - (time_mn % interval)
        formatted_time = Time.at(minimum * Util::SEC_IN_MIN).to_i
    end

    # just print the progression,
    # since we can not print every records(too much) we have to
    # print like every ten percetn or something
    def progression counter,opts = {}
        (@steps = nil; return) if opts[:reset]
        @steps ||= (1..10).map{ |n| [(@num_rows.to_f / 10.to_f) * n,n*10] }
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
            ## we only record unique ID, not 1000x 
            @processed_ids = Set.new
            @total = 0 # total number of row accepted
        end
            
        def compute
            counter = 0.0
            counter_proc = Proc.new { |n| @num_rows = n }
            @db = Database::Mysql.default
            @monitors.each  do |monitor|
                @current = monitor
                @current.sources.each do |source|
                    h = { proc: counter_proc }
                    h[:union] = true if @opts[:union]
                    source.schema.new_records(@current,h) do |row|
                        analyse_row row
                        counter += 1
                        progression(counter)
                    end
                    @current.schema.insert_stats source
                    @current.reset_stats
                    @current.schema.processed_files source, @processed_ids.to_a
                    @processed_ids = Set.new
                    Logger.<<(__FILE__,"INFO","Analyzed & Accepted #{@total}/#{counter} records for #{source.name}")
                    @total = 0
                    @num_rows = 0
                    progression(0,reset: true)
                end
            end
        end
        # analyse a single record for a single monitor
        def analyse_row row
            return if @current.filter && !@current.filter.filter_row(row)
            record_time = get_time_from_record row
            time = get_formatted_time record_time

            @current.stats.analyze time,row
            ## this id has been processed
            @processed_ids << row[:file_id]
            @total += 1
        end


        private
        def get_time_from_record record
            field_time = @flow.time_field_records
            record[field_time]
        end

    end
end

