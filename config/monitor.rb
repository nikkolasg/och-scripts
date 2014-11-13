module  App

    class Monitor

        [:input,:output].each do |f|
            str = "def #{f}(*param)
                    if param.size > 0
                        @#{f} = @#{f} + param.map {|p| p.downcase}
                    else
                        @#{f}
                    end
                  end"
                        Monitor.class_eval(str)
        end
        alias :inputs :input
        alias :outputs :output

        [:time_interval,:filters,:flow,:table_stats].each do |f|
            Monitor.class_eval(App.define_accessor(f))
        end

        Monitor.class_eval(App.define_inout_reader(:table_records))
        Monitor.class_eval(App.define_inout_reader(:table_records_union))
        Monitor.class_eval(App.define_accessor(:table_records_backlog))

        def initialize flow
            @flow = flow
            @input = []
            @output = []
            @filters = {}
            @filter_records = flow.records_allowed
        end       

        def name (param = nil)
            if param
                @name = param
                @table_stats = "MON_#{@flow.name}_#{@name.upcase}"
                # used to store all the records analysed by this mon
                @table_records = @table_stats + "_RECORDS"
                @table_records_union = @table_records + "_UNION"
                @table_records_backlog = @table_stats + "_BACKLOG"
            else
                return @name
            end
        end
        # only aggregate specific records
        def filter_records *args
            if args.size > 0 ## affectation
                @filter_records =  args
            else
                @filter_records
            end
        end
        # only aggregate records where the block for this field
        # return true
        def filter_where field,&block
            filters[field.downcase.to_sym] = block
        end

        # return the column name for a given record name
        # depending on the direction
        def column_record name,dir = nil
            if dir
                if dir == :input
                    return  name + "_IN"
                elsif dir == :output
                    return  name + "_OUT"
                end
            end
            [name+"_IN",name+"_OUT"]
        end
        # return ALL the columns for the records table
        # with the corresponding sql statement
        def stats_columns
            h = { App.database.timestamp => "INT UNSIGNED UNIQUE DEFAULT 0" }
            h2 = @filter_records.inject({}) do |col,rec|
                var = column_record(rec)
                col[var[0]] = "INT DEFAULT 0"
                col[var[1]] = "INT DEFAULT 0"
                col
            end
            h.merge! h2
            return h
        end

        # return the switches where this monitor looks up
        def switches
            @switches ||= self.send(:sources).map do |s|
                s.switches
            end.flatten(1).uniq
        end

        def sources
            @sources ||= (@input + @output).flatten.reduce([]) do |col,so|
                    s = @flow.sources(so)
                    col << s if s
                    col
            end
        end

        def to_s
            return @name
        end
    end



end
