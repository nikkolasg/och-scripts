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