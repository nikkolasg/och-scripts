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
                gcr = fields[:global_call_reference]
                cr = fields[:call_reference]
                hash[:fields] = rename_fields fields,@fields2change 
                hash[:values].each do |row|
                   row[gcr] = row[gcr].upcase if gcr
                   row[cr] = row[cr].upcase if cr
                   transform_time_values row,*hash[:fields].values_at(*@fields2change.values)
                end
            end

        end

    end

end
