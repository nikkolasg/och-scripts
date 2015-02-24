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