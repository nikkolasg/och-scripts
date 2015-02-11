module Mapper

    class ProceraMapper < GenericMapper
        require 'time'
        def initialize opts = {}
            super opts
            @timefields = [:start_time,:end_time]
            @fields2change = map_time_fields @timefields
        end

        def map_json json
            json.each do |type,hash|
                hash[:values].each do |record|
                    @timefields.each do |tf|
                        ind = hash[:fields][tf]
                        next unless ind
                        ## format the time in timestamp
                        record[ind] = Time.parse(record[ind]).to_i
                    end
                end
            end
        end

        def map_fields fields
            @timeindexes = []
            @timefields.each do |f|
                @timeindexes << fields[f]
            end
            rename_fields fields,@fields2change
        end

        def map_record record
            @timeindexes.each do |i|
                record[i] = Time.parse(record[i]).to_i
            end
        end
    end
end
