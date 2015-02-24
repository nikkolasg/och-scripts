#
# Copyright (C) 2014-2015 Nicolas GAILLY for Orange Communications SA, Switzerland
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
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
            @filters.inject(Set.new) { |col,(k,v)| col.merge(k) unless v.empty? ; col}.to_a
        end

        ## you can pass another filter and it will merge
        # the two in one. in case of 2 blocks for one field,
        # both block will be executed
        def merge_filter filter
            filter.filters.each do |names,arr|
                @filters[names] +=  arr ## adds all  blocksfor this particular field
            end 
        end

        ## set a rule for theses names
        def field *names, &block
            if block
                @filters[names] << block
            else ## only affectation of fields
                @filters[names] << true
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
                hash[:fields] = fields = filter_fields fields
                hash[:values] = values.keep_if do |record|
                    filter_record fields,record
                end
            end
        end

        ## Filter out some fields, 
        ###USELESS
        def filter_fields fields
            fields_ = fields_allowed
            fields.keep_if do |field,index|
                fields_allowed.include?(field)
            end
            return fields
        end

        # filter out fields AND record at same time.
        # If field is not present in filter, discard the value from the record
        # and discard the field too.
        #
        def filter_record fields,record
            found = false
            allowed = true
            @filters.each do |ffields,actions|
                arr_ind = fields.values_at(*ffields)
                next if arr_ind.any? { |n| n.nil? }
                arr = record.values_at(*arr_ind)
                next if arr.any? { |n| n.nil? }
                found = true
                allowed &= evaluate(actions,arr)
                return false unless allowed
                next true
            end
            return allowed & found
        end

        ## Filter out a ROW from the RECORDS table in the db
        ## No need to pass by filter_fields before, since 
        # the row is Hash and have already the symbolized fields as key
        def filter_row row
            found = false
            allowed = true
            @filters.each do |ffields,actions|
                arr = row.values_at(*ffields)
                next if arr.any? { |n| n.nil? }
                found = true
                allowed &= evaluate(actions,arr)
                return false unless allowed
                next true
            end
            return allowed & found
        end

        private


        ## evaluate all filters for a given field, for this value
        # return true if all evaluates to true
        # otherwise false
        def evaluate actions,array
            actions.all? do |action|
                next action.call(*array) if action.is_a?(Proc)
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