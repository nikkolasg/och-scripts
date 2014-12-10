module Mapper

    class Nkm15Mapper < GenericMapper

        def initialize opts = {}
            super(opts)
            @timefields = [ :in_channel_allocated_time,
                               :charging_start_time,
                               :charging_end_time,
                               :call_reference_time,
                               :time ]
            @fields2change = map_time_fields @timefields
        end

        def map_json json
            json.each do |name,hash|
                fields = hash[:fields]
                hash[:fields] = rename_fields fields,@fields2change 
                hash[:values].each do |row|
                   transform_time_values row,*hash[:fields].values_at(*@fields2change.values)
                end
            end

        end

    end

end
