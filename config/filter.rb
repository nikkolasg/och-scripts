module Conf
    require_relative '../debugger'
    ## starting block of the DSL for the filter 
    def filter &block
        if block_given?
            @filter = Filter.new self
            @filter.instance_eval(&block) if block
            #Debug::debug_filter_fields_allowed @filter.filters
        else
            return @filter
        end
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
    end

    ## class that handles all the filtering in the application
    class Filter
        attr_reader :parent,:filters

        
        def initialize  parent
            @filters = {}
            @parent = parent
            @filtered_fields = []
            set_fields2keep
        end

        def fields_allowed
            @filters.keys
        end

        ## set a rule for theses names
        def field *names, &block
            if block
                raise "Filter can only take a block for one field at a time" unless names.size == 1
                @filters[names.first.downcase.to_sym] = block
            else ## only affectation of fields
                names.each do |name|# simply returns true all time =)
                    @filters[name.downcase.to_sym] = true
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
                hash[:fields] = filter_fields(fields)
                hash[:values] = values.inject([]) do |col,record|
                    v = filter_record record
                    col << record if v
                    col
                end
            end
        end


        ## Filter out a pair Field / Value
        #  Its only here for optimization , speed
        #  since we could do fine with the rest but
        #  for mms decoder, we have already that pair present
        #  so it's faster this way
        def filter_pair field, value
            v = get_value(field)
            return false unless v
            return true unless v.is_a?(Proc)
            return v.call(value)
        end

        ## Filter out some fields, and registers the good
        #one. To call BEFORE filter_record,so it knows the fields to keep
        def filter_fields fields
            @filtered_fields = []
            fields.keep_if do |field,index|
                v = get_value(field)
                @filtered_fields << [field,index] if v
                v
            end
            return fields
        end

        ## Retreive the value associated with this field
        # IF ANY =)
        # Also, manage the Timestamp fields that are prefixed or not
        def get_value field
            f = field.to_s.sub(Util::TIME_PREFIX,"")
            return @filters[f.to_sym]
        end
        # filter out a record from the JSON output decoder
        # based on the filtering fields it has recorded
        # IF no corresponding fields are found, reject the record
        def filter_record record
            return false if @filtered_fields.empty?
            allowed = true 
            found = false
            @filtered_fields.each do |field,index|
                next unless ( v = get_value(field))
                ## if its not a proc it's a true value so no need to apply
                found ||= true
                allowed &= evaluate(v,record[index])
                return false unless allowed
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
                next unless (v = get_value(field))
                found ||= true
                allowed &= evaluate(v,value)
                return false unless allowed
            end
            return allowed & found
        end
        
       private
        

      def evaluate block, value
            return true unless  block.is_a?(Proc)
            return block.call(value) if value
            return false
      end 

      ## Allow some fields to always be taken, accepted
      #  like the field which gives us the timing ;)
      #  (instead of putting it in every filter)
      def set_fields2keep
        if @parent.is_a?(Flow)
            self.send(:field,@parent.time_field_records)
        elsif @parent.is_a?(Monitor)
            self.send(:field,@parent.flow.time_field_records)
        end
      end

    end
end
