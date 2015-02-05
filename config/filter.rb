module Conf
    require_relative '../debugger'
    ## starting block of the DSL for the filter 
    def filter &block
        if block_given?
            @filter = Filter.new self
            @filter.instance_eval(&block) if block
        end
        return @filter
    end 
    
    # simple filter that does nothing. 
    # Just to keep it dry, no need to check existence of
    # filter each time 
    class NullFilter
        def filter_fields fields
            return fields
        end
        def filter_records record
            return true
        end
        def filter_row row
            return true
        end
        def filter_pair field,value
            return true
        end
    end

    ## class that handles all the filtering in the application
    class Filter
        attr_reader :parent,:filters

        def initialize  parent
            @filters = Hash.new { |h,k| h[k] = [] }
            @parent = parent
            @filtered_fields = []
            set_fields2keep
        end

        def fields_allowed
            @filters.inject([]) { |col,(k,v)| col << k unless v.empty? ; col}
        end

        ## you can pass another filter and it will merge
        # the two in one. in case of 2 blocks for one field,
        # both block will be executed
        def merge_filter filter
           filter.filters.each do |name,arr|
                @filters[name] +=  arr ## adds all  blocksfor this particular field
           end 
        end

        ## set a rule for theses names
        def field *names, &block
            if block
                raise "Filter can only take a block for one field at a time" unless names.size == 1
                @filters[names.first.downcase.to_sym] << block
            else ## only affectation of fields
                names.each do |name|# simply returns true all time =)
                    @filters[name.downcase.to_sym] << true
                end
            end
        end
        alias :fields :field
        # instead of enumerating all fields, we can pass
        # a dump file ! like value:sql field, 
        # a dump file ! like value:sql field, 
        def fields_file file_name
            path = Conf::directories.app + "/" + file_name
            raise "File in fields_file does not exists !!" unless ::File.exists?(path)
            require_relative '../decoders/dump'
            h = MysqlDumper::read_dump_file path
            h.each do  |field,sql|
                self.field(field)
            end
        end

        #filter out an entire json
        def filter_json json
            json.each do |name,hash|
                fields = hash[:fields]
                values = hash[:values]
                hash[:fields] = fields = filter_fields(fields)
                hash[:values] = values.keep_if do |record|
                    filter_record fields,record
                end
            end
        end

               ## Filter out a pair Field / Value
        #  Its only here for optimization , speed
        #  since we could do fine with the rest but
        #  for mms decoder, we have already that pair present
        #  so it's faster this way
        def filter_pair field, value
            filters = get_filters(field)
            return false unless v.empty?
            return evaluate(filters,value)
        end

        ## Filter out some fields, 
        def filter_fields fields
            fields.keep_if do |field,index|
                get_filters(field).empty? ? false : true
            end
            return fields
        end

        # filter out fields AND record at same time.
        # If field is not present in filter, discard the value from the record
        # and discard the field too.
        #
        def filter_record fields,record
            return false if fields.empty?
            allowed = true 
            found = false
            fields.select! do |field,index|
                ## if this field is not needed, remove the value from the record
                if ( filters = get_filters(field)).empty?
                    #record.delete_at(index)
                    next false
                end
                found ||= true
                allowed &= evaluate(filters,record[index])
                return false unless allowed
                next true
            end
            return allowed & found
        end

        ## Filter out a ROW from the RECORDS table in the db
        ## No need to pass by filter_fields before, since 
        # the row is Hash and have already the symbolized fields as key
        def filter_row row
            found = nil
            allowed = true
            row.each do |field,value|
                next unless (filters = get_filters(field)).empty?
                found ||= true
                allowed &= evaluate(filters,value)
                return false unless allowed
            end
            return allowed & found
        end
        
       private

       ## Retreive the value associated with this field
       # IF ANY =)
       # Also, manage the Timestamp fields that are prefixed or not
       def get_filters field
           f = field.to_s.start_with?(Util::TIME_PREFIX) ? field.to_s.sub(Util::TIME_PREFIX,"") : field
           return @filters[f.to_sym]
       end


       ## evaluate all filters for a given field, for this value
       # return true if all evaluates to true
       # otherwise false
       def evaluate filters,value
           filters.all? do |f|
               next f.call(value) if f.is_a?(Proc)
               ## if no proc, then it is true value
               next true
           end  
       end


       ## Allow some fields to always be taken, accepted
       #  like the field which gives us the timing ;)
       #  (instead of putting it in every filter)
       def set_fields2keep
           if @parent.is_a?(Flow)
               self.send(:field,@parent.time_field_records)
           elsif @parent.is_a?(Monitor)
               self.send(:field,@parent.time_field || @parent.flow.time_field_records)
           end
       end

    end
end
