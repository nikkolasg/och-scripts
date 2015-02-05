module Mapper

    ## simply change the time field value
    class PGWMapper < GenericMapper
        
        def initialize opts = {}
            super(opts)
            @fields2change = map_time_fields([:start_time]) 
        end

        def map_json json
            json.each do |type, hash|
                hash[:fields] = map_fields hash[:fields]
                hash[:values].each { |record| map_record hash[:fields],record}
            end
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

