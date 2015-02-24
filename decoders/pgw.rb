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
module Decoder
    require 'csv'
    require_relative '../debugger'

    ## Decoder for the output of PGW
    class PGWDecoder    
        include Decoder
        
        def initialize opts = {}
            @opts = opts
            @sep = ";"
            @records_fields = Hash[CSVFields::PGW_FIELDS.each_with_index.map { |v,i| 
                [v,i] }]
        end

        def dump_file
            @dump_file ||= File.dirname(__FILE__) + "/pgw_dump"
        end

        def test_file
            @test_file ||= File.dirname(__FILE__) + "/pgw_test"
        end

        def decode file, opts = {}
            check file
            @records_fields = @mapper.map_fields(@records_fields) if @mapper
            @records_fields = @filter.filter_fields(@records_fields) if @filter
            
            json = { "PGW" => { fields: @records_fields, values: [] } }
            ::CSV.foreach(file.full_path,{ col_sep: @sep }) do |record|
                record = record.map { |c| c ? c : "" }
                @mapper.map_record(@records_fields,record) if @mapper
                if (!@filter || (@filter.filter_record(@records_fields,record)))
                    json["PGW"][:values] << record
                end
            end
            sanitize_json json
            Debug::debug_json json if @opts[:d]
            json
        rescue => e
            Logger.<<(__FILE__,"ERROR","Error PGW CSV Decoding #{file.full_path}: #{e.message}")
            return json
        end
    end

end