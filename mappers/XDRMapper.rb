module Mapper

    class XDRMapper < GenericMapper
        
        def initialize opts = {}
            super(opts)
        end
      
        def map_json json
            json.each do |name,hash|
                map_fields hash[:fields]
                fields = hash[:fields]
                hash[:values].select! do |record|
                    map_record fields,record
                end
            end
        end

       ## rename the time fields into timestamp 
        def map_fields fields
            @idate = fields[:charging_date]
            @itime = fields[:charge_time]
            rename_fields fields,map_time_fields([:charging_date])
        end

        ## Only map the time & date fields together
        # return true if it has found a normal value
        # otherwise false
        def map_record fields, record
            record[@idate] = "20" + record[@idate] + record[@itime] 
            transform_time_values record,@idate
            record[@idate] == 0 ? false : true
        end

    end

end
