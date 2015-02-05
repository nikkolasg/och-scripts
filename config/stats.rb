module Conf

    class Monitor
        def stats &block
            if block_given?
                @stats = Stats.new self
                @stats.instance_eval(&block) if block
                instance_variable_set(:@stats,@stats)
            else
                @stats
            end
        end
    end

    class Stats
        attr_reader :stats
        RECORDS_LISTING_KEY = :listing
        def initialize parent = nil
            @stats = Hash.new
            @parent = parent
        end


        ## return the fields used by the monitor in the source records table
        #  i.e. the fields needed to retrieve to make the stats
        #  and to pass filter
        def fields
            @fields ||= @stats.keys.select{ |k| k != RECORDS_LISTING_KEY}.flatten.uniq
        end

        ## return the name of the columns used in the stats table
        def columns
            @columns ||= @stats.values.flatten.map { |a| a.column_name }
        end


        ## Define the operations method available.
        #each one create a class that will handles the stats
        #
        #For sum & mean, you specify the col_name tht you want in the db,
        #the field you want to sum on, 
        #and any optionals fields that will be passed in the block 
        #for filtering
        def sum col_name,field2sum,*fields, &block
            key = key_fields [field2sum] + fields
            @stats[key] = []  unless @stats[key]
            @stats[key] << SumField.new(col_name,block)
        end

        def list col_name, *fields, &block
            key = key_fields(fields)
            @stats[key] = [] unless @stats[key]
            @stats[key] << ListField.new(col_name,block)
        end

        ## used to count all the records without specific fields, or blocks
        def list_records
            @stats[RECORDS_LISTING_KEY] = [] unless @stats[RECORDS_LISTING_KEY]
            @stats[RECORDS_LISTING_KEY] << ListField.new(RECORDS_LISTING_KEY,nil)
        end

        def mean col_name, field2mean,*fields,&block
            key = key_fields [field2mean] + fields
            @stats[key] = [] unless @stats[key]
            @stats[key] << MeanField.new(col_name,block)
        end 

        def key_fields fields
            return fields.map {|f|f.downcase.to_sym}
        end
        private :key_fields


        def analyze *args

            if args.size == 2
                analyze_row *args
            elsif args.size == 3
                analyze_record *args
            end
        end

        def analyze_row formatted_time,row
            analyze_listing formatted_time
            @stats.each do |fields,actions|
                arr = row.values_at(*fields)
                next if arr.any? {|v| v.nil? } # if nil value => no field
                actions.each { |a| a.save(formatted_time,arr) }
            end
        end


        def analyze_record formatted_time,fields,record
            analyze_listing formatted_time
            @stats.each do |sfields,actions|
                arr_ind = fields.values_at(*sfields)
                next if arr_ind.any? { |n| n.nil? } ## lookup index
                arr = record.values_at(*arr_ind)
                next if arr.any? { |v| v.nil? } ## lookup values
                actions.each { |a| a.save(formatted_time,arr) }
            end
        end

        # return the global counts for each field / actions &
        # to insert in the db
        # Key : Timestamp
        # Value : Hash ==> Key : collumn_name
        #                  Value : value
        def summary
            stats = Hash.new { |h,k| h[k] = {} }
            @stats.each do |field, actions|
                actions.each do |action|
                    col = action.column_name
                    ## register the value of this "stats" for the column name
                    action.iterate do |ts,value|
                        stats[ts][col] = value
                    end
                end
            end
            return stats
        end

        def reset
            @stats.each do |f,actions|
                actions.each { |a| a.reset }
            end
        end

        private

        ## simply call the listing here, because they dont need any fields like others stats
        def analyze_listing formatted_time
            return unless @stats[RECORDS_LISTING_KEY]
            @stats[RECORDS_LISTING_KEY].each do |action|
                action.save(formatted_time,nil)
            end
        end


    end

    ## simple abstract class
    class ActionField
        attr_reader :column_name, :value
        def initialize col
            @column_name = col.to_sym
            @value = Hash.new { |h,k| h[k] = 0 }
        end

        def reset
            @value.clear
        end

        ## proxy method for iterating over the values
        def iterate
            @value.each do |ts,value|
                yield ts,value
            end
        end

    end

    ## List the entry t
    class ListField < ActionField

        def initialize col, block 
            super(col)
            @block = block
        end

        def save formatted_time,arr
            v = @block ? @block.call(*arr) : true
            @value[formatted_time] += 1 if v
            #puts "#{@column_name} : #{arr} ==> #{v}"
        end

    end

    ## sum the entries values
    class SumField < ActionField
        def initialize col,block
            super(col)
            @block = block
        end


        ## the time corresponding, and an arary of values to sum
        #you can sum multiple columns into one
        def save formatted_time,arr
            # add it to the sum
            arr.each do |n| 
                next unless @block.call(*arr[1..-1]) if @block
                i = Integer(n) rescue return
                @value[formatted_time] += i;
            end
        end
    end

    ## will do as the sum but will render the mean 
    class MeanField < ActionField
        def initialize col,block
            super(col)
            @value = Hash.new { |h,k| h[k] = [0,0] }
            @block = block
        end

        def save formatted_time,arr
            arr.each do |n|
                next unless @block.call(*arr[1..-1]) if @block
                i = Integer(n) rescue return
                @value[formatted_time][0] += i
                @value[formatted_time][1] += 1
            end
        end

        def iterate 
            @value.each do |ts,arr|
                ## cannot divide by 0
                arr[1] == 0 ? yield(ts,0) : yield(ts, arr[0]/arr[1])
            end 
        end 
    end
end
