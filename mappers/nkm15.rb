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
                hash[:values].each do |row|
                   transform_time_values row,*fields.values_at(*@timefields)
                end
                hash[:fields] = rename_fields fields,@fields2change 
            end

        end

    end

end
