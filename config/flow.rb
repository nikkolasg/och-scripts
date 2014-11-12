module App

    class Flow
        [ :name,:decoder,:test_file,:time_field_records,:table_records_union,:table_cdr_union].each do |f|
            Flow.class_eval(App.define_accessor(f))
        end
        ## create custom accessor when the direction is supplied
        # u can call it like this
        # flow.table_cdr(:input) ==> CDR_MSS
        # flow.table_cdr         ==> CDR_MSS (input is default)
        # flow.table_cdr(:output)==> CDR_MSS_OUT
        # flow.table_cdr "CDR_MSS" ==> affectation !
        [ :table_cdr,:table_records ].each do |f|
            str = "def #{f}(param=nil)
                    if param
                        if :input == param
                            return @#{f}
                        elsif :output == param
                            return @#{f} + App.database.output_suffix
                        else 
                            @#{f} = param.upcase
                            @#{f}_union = @#{f} + '_UNION'
                        end
                    end
                    @#{f} 
               end"
                            Flow.class_eval(str)
        end 
        Flow.class_eval(App.define_inout_reader(:table_records_union))
        ## Create custom access
        #   when specified cdr_fields_file , it will
        #   trigger the reading of the file 
        #   and put the fields into cdr_fields !!
        [:cdr_fields,:records_fields].each do |f|
            str = "def #{f.to_s + "_file"}(param=nil)
                        if param
                            @#{f.to_s + "_file"} = param
                            @#{f} = parse_fields_file(param)
                        else
                            @#{f.to_s + "_file"}
                        end
                   end"
                            ## define the fields attributes
                            Flow.class_eval(App.define_accessor(f))
                            Flow.class_eval(str)
        end



        def initialize name
            @name = name
            @sources = []
            @monitors = []
            @records_allowed = []
            @table_records = "RECORDS_" + name.to_s
            @table_cdr = "CDR_" + name.to_s
        end
        def source  name, &block
            newSource = Source.new(self)
            newSource.name (name.downcase.to_sym)
            @sources << newSource
            newSource.instance_eval(&block)
        end

        def sources search = nil
            if search ## if we want specific source
                if [:input,:output].include?( search)
                    # we want sources from a given direction
                    return @sources.select { |s| s.direction == search }
                else
                    # we want source from a given name
                    search = search.downcase.to_sym
                    return @sources.select { |s| s.name == search }.first
                end
            else
                @sources
            end
        end

        ## return all the switches for this flow
        #  
        def switches
            @switches ||= @sources.map { |s| s.switches }.flatten(1).uniq
            @switches
        end

        def records_allowed *args
            if args.size == 0 ## no args, ==> accesssor
                @records_allowed
            else ## affectation
                @records_allowed +=  args
            end
        end

        def monitor name, &block
            m = Monitor.new self
            m.name name.downcase.to_sym
            @monitors << m
            m.instance_eval(&block)
        end
        # accesor of monitors by name or all the monitors
        def monitors name = nil
            if name
                name = name.downcase.to_sym
                return  @monitors.select { |m| m.name == name }.first
            else
                return @monitors
            end
        end


        private

        # read the fields, and create custom accessor for it
        # file must be in format
        # column_name:SQL TYPE
        # output format is a Hash
        # key => column_name
        # value => sql type
        def parse_fields_file file
            hash = {}
            File.read(file).split("\n").each do |line|
                field,sql = line.split ':'
                hash[field] = sql
            end
            hash
        end
    end


end
