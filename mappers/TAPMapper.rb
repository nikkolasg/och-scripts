module Mapper
    require 'date'
    class TAPMapper < GenericMapper
        
       def initialize opts = {}
          super(opts)
          @fields2change = map_time_fields([:start_time])
       end

       def map_fields fields
           @idate = fields[:start_date]
           @itime = fields[:start_time]
           @ioffset = fields[:start_utc_offset]
           rename_fields fields,@fields2change
           fields
       end

       def map_record record
            d = ::DateTime.parse(record[@idate]+record[@itime])
            d.new_offset(record[@ioffset])
            record[@itime] = d.strftime("%Y%m%d%H%M%S")
            transform_time_values record,@itime
            record
       end

       ## Will concatenate start_date+start_time and
       #put it in start_time
       def map_json json
            json.each do |type,hash|
                idate = hash[:fields][:start_date]
                itime = hash[:fields][:start_time]
                hash[:values].each do |record|
                    record[itime] = record[idate]+record[itime]
                    transform_time_values record,itime
                end
                rename_fields hash[:fields],@fields2change
            end
            json
       end

    end

end
