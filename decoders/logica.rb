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

    ## It's a bit like CSV, but needs to be treated differently
    ## FOR SMS DECODING =)
    class LogicaDecoder
        include Decoder
        require 'csv'

        DUMP_FILE = "logica_dump"
        TEST_FILE = "logica_test"
        DECODER = Conf::directories.app + "/decoding/logica.pl -dc "
        def initialize opts = {}
            @opts = opts
            @sep = "|"
        end

        def decode file
            check file 
            cmd = DECODER + "\"#{file.full_path}\""
            out = exec_cmd(cmd)
            json = { "SMS" => { :values => [] } }
            CSV.parse(out,col_sep: @sep) do |row|
                if !json["SMS"][:fields] ## fields
                    h = Hash[row.compact.each_with_index.map {|v,i| [v,i]}] 
                    json["SMS"][:fields] = RubyUtil::symbolize(h)
                else ## values
                    json["SMS"][:values] << row.map { |v| v.nil? ? '' : v }
                end
            end
            Debug::debug_json json if @opts[:d]
            json = @mapper.map_json(json) if @mapper
            json = @filter.filter_json(json) if @filter
            Debug::debug_json json if @opts[:d]
            sanitize_json(json) 
        end
    end

end