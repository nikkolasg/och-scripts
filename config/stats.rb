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
        
        def initialize parent = nil
            @stats = Hash.new
            @parent = parent
        end
        
        ## return the fields used by the monitor in the source records table
        #  i.e. the fields needed to retrieve to make the stats
        #  and to pass filter
        def fields
            @fields ||= @stats.keys.flatten.uniq
        end

            ## return the name of the columns used in the stats table
        def columns
            @columns ||= @stats.values.flatten.map { |a| a.column_name }
        end


        ## supposed to be one , but we never know
        def sum col_name,*field
            key = key_fields field
            @stats[key] = []  unless @stats[key]
            @stats[key] << SumField.new(col_name)
        end
      
        def list col_name, *fields, &block
            key = key_fields(fields)
            @stats[key] = [] unless @stats[key]
            @stats[key] << ListField.new(col_name,block)
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
            @stats.each do |fields,actions|
                arr = row.values_at(*fields)
                next if arr.any? {|v| v.nil? } ## if nil value => no field
                actions.each { |a| a.save(formatted_time,arr) }
            end
        end


        def analyze_record formatted_time,fields,record
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
                    action.value.each do |ts,value|
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

    end

    ## simple abstract class
    class ActionField
        attr_reader :column_name, :value
        def initialize col
            @column_name = col
            @value = Hash.new { |h,k| h[k] = 0 }
        end

        def reset
            @value = Hash.new { |h,k| h[k] = 0 }
        end

    end

    class ListField < ActionField

        def initialize col, block
            super(col)
            @block = block
        end

        def save formatted_time,arr
             v = @block.call(*arr)
             @value[formatted_time] += 1 if v
             #puts "#{@column_name} : #{arr} ==> #{v}"
        end

    end

    class SumField < ActionField

        def initialize col
            super(col)
        end

        def save formatted_time,arr
            # add it to the sum
            arr.each { |n| @value[formmatted_time] += Integer(n) rescue return }
        end
    end
end
