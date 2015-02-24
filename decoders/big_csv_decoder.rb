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
    require_relative 'csv'
    require_relative '../debugger'
    require 'zlib'
    ## Will NOT decompress the whole file,
    #but will use the gzip reader stream ,so read line by line
    ## it will yield each line (mapped & formatted)
    #
    ### The MAPPER MUST have a filter_field & filter_record functions !!
    #   otherwise it can not yield the right values to the inserter
    class BigCSVDecoder < CSVDecoder

        def decode file,&block
            @nb_line = 0
            @fields = get_fields(nil)
            @fields = @mapper.map_fields(@fields) if @mapper
            @fields = @filter.filter_fields(@fields) if @filter
            @count = 0
            @json = { @type => { :fields => @fields, :values => [] } }
            file_ = file.zip? ? ::Zlib::GzipReader.open(file.full_path) : File.open(file.full_path,"r")
            SignalHandler.ensure_block { Logger.<<(__FILE__,"WARNING","SIGINT Catched : Stoppong decoding file #{file.full_path}"); file_.close }
            lines = file_.lines
            iterate_over lines,&block
            file_.close
            unless block_given?
                sanitize_json @json
                return @json
            end
        rescue => e
            Logger.<<(__FILE__,"ERROR","Error BigCSV Decoder file #{file.full_path}: #{e.class} : #{e.message}")
            raise e
        end

        def iterate_over lines,&block
            lines.next if @opts[:skip_header] 
            lines.each do |line|
                @record = line.chomp.split(@sep).map { |c| c ? c : "" }
                @nb_line += 1
                break unless analyze(&block) 
                SignalHandler.check { }
            end
        end
        def analyze  &block
            #Debug::debug_fields_and_record @fields,@record 
            @mapper.map_record(@record) if @mapper
            keep = @filter.filter_record(@fields,@record) if @filter
            ## if record is good to keep or if there is no filter ...
            if keep || !@filter 
                if block_given?
                    yield @fields,@record 
                else 
                    @json[@type][:values] << @record
                end
            end
            @count += 1
            @opts[:nb_line] ? (@count > @opts[:nb_line] ? false : true ) : true 
        end

    end
end