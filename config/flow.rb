module Conf

    class Flow
        include Conf
        require_relative 'source'
        require_relative 'monitor'
        [ :name].each do |f|
            Flow.class_eval(Conf.define_accessor(f))
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
            newSource = Conf::Source.new(name.downcase.to_sym,self)
            @sources << newSource
            newSource.instance_eval(&block)
            newSource.apply_filter
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

        def time_field_records param = nil
            if param
                @time_field_records = (Util::TIME_PREFIX + param.downcase.to_s).to_sym
            else
                @time_field_records
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
            m = Conf::Monitor.new self,name.downcase.to_sym
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

    end

end
