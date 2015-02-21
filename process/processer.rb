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
            Logger.<<(__FILE__,"INFO","Having processed #{perc}%  now ...",inline: true)
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
            @source = opts[:source] || nil
        end
           

       # main method !! 
        def compute
            counter = 0.0
            total_counter = 0
            counter_proc = Proc.new { |n| @num_rows = n }
            @db = Database::Mysql.default
            ## process each monitors separatly
            @monitors.each  do |monitor|
                @current = monitor
                ## for each source, look what they've got
                @current.sources.each do |source|
                    next if @source && @source != source
                    source.set_options @opts
                    h = { proc: counter_proc }
                    ## if we want to seelect from union or not
                    h[:union] = true if @opts[:union]
                    ## return a row from the db to be analyzed
                    source.schema.new_records(@current,h) do |row|
                        analyse_row row
                        counter += 1
                        total_counter +=1
                        Logger.<<(__FILE__,"INFO","Processed #{total_counter} records for now ...",inline: true) if total_counter % 10000 == 0 #: progression(counter)
                        SignalHandler.check { 
                            @db.close; 
                            Logger.<<(__FILE__,"WARNING","Exit catched. Abort.")
                        }
                        ## FIX : schema will reconnect, but it shouldn't 
                        #as it will mess up with the query still not finished.
                       # ## in case we have many many stats ... 
                        ### cant hold all , we have to let go sometimes !!
                        #if counter > 1000000
                            #@db.connect { @current.schema.insert_stats(source); @current.reset_stats; @current.schema.processed_files(source,@processed_ids.to_a) }
                            #counter = 0
                        #end
                    end
                    @db.connect do
                        @current.schema.set_db @db
                        ## insert and reset stats
                        @current.schema.insert_stats source
                    end
                    @current.reset_stats
                    @current.schema.processed_files source, @processed_ids.to_a

                    Logger.<<(__FILE__,"INFO","Analyzed & Accepted #{@total}/#{total_counter} records for #{source.name}")
                    @processed_ids = Set.new
                    @total = 0
                    @num_rows = 0
                    progression(0,reset: true)
                    SignalHandler.check { @db.close; Logger.<<(__FILE__,"WARNING","SIGINT catched. Abort.") }
                end
            end
        end

        # analyse a single record for a single monitor
        def analyse_row row
            if @current.filter 
                res = @current.filter.filter_row(row)
                return unless res
            end
            record_time = get_time_from_record row
            time = get_formatted_time record_time
            @current.stats.analyze time,row
            ## this id has been processed
            @processed_ids << row[:file_id]
            @total += 1
        end


        private
        def get_time_from_record record
            field_time = @current.time_field || @flow.time_field_records
            record[field_time]
        end

    end
end

