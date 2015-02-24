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