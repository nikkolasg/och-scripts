module Mapper

    class PGWMapper < GenericMapper
        
        def initialize opts = {}
            super(opts)
            @fields2change = map_time_fields([:start_time]) 
        end

        def map_fields fields
            rename_fields fields, @fields2change
        end

        def map_record fields,row
            ind = fields[@fields2change[:start_time]] 
            transform_time_values row,ind
        end

    end

end

