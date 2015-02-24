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

    class CSVDecoder
        require 'csv'
        require_relative '../debugger'
        include Decoder

        DEFAULT_SEP = ";"

        attr_accessor :sep

        def initialize opts = {}
            @opts = opts
            @sep = opts[:separator] || DEFAULT_SEP
            @type = @opts[:type]
            @FIELDS,@RECORDS = CSVFields::retrieve(@type)
            @csv_opts = { col_sep: @sep, :unconverted_fields => "" }
        end

        def dump_file
            @dump_file ||= File.dirname(__FILE__) + "/" + @type.downcase.to_s + "_" + "dump"
        end

        def test_file
            @test_file ||= File.dirname(__FILE__) + "/" + @type.downcase.to_s + "_" + "test"
        end

        ## main method
        #
        # if you call it like this, it will return a JSON structure
        # Type of record ==> :fields => field : index
        #                    :values => matrix of values, each line is a record
        #
        # You can also call it with a block, then the method
        # will yield the fields hash, and the record just analyzed
        def decode file,&block
            check file
            @nb_line = 0
            fields = nil
            @json = Hash.new { |h,k| h[k] = {} }
            fhandler = File.open(file.full_path,"r") do |file_|
                lines = file_.lines # enumerator
                lines.next if  @opts[:skip_header] ## if we want to skip first line
                lines.each do |line|
                    @nb_line += 1
                    ## remove nils elements ...
                    record = line.chomp.split(@sep).map { |c| c ?  c : "" }
                    ## Proc.new without block wraps the block given to the decode meth
                    break unless analyze(record,&block)               
                    SignalHandler.check { Logger.<<(__FILE__,"WARNING","Stopping decoding file #{file.full_path}"); file_.close }
                end
            end
            finalize_json
        rescue => e
            Logger.<<(__FILE__,"ERROR","Error CSV Decoding file #{file.full_path}: #{e.message}")
            return nil
        end

        ## MAP & FILTER & SANITIZE AND RETURN JSON
        def finalize_json 
            sanitize_json @json
            @json = @mapper.map_json(@json) if @mapper
            Debug::debug_json @json if @opts[:d]
            @json = @filter.filter_json(@json) if @filter
            Debug::debug_json @json if @opts[:d]
            @json
        end

        ## INSERT in JSON or yield etc
        def analyze record
            # type of record
            if @RECORDS.empty? ## if a file only describe one type of record
                name = @type
            else ## otherwise we pick up the name of the record being analyzed
                code = record.first.to_i
                name = @RECORDS.key(code)
                return true unless name # if we have no idea what this records is ...
            end
            # first time we see this record
            # so we take its fields name
            if !@json[name][:fields]
                @json[name][:fields] = get_fields code
                @json[name][:values] = []
            end

            @json[name][:values] << record
            

            Logger.<<(__FILE__,"DEBUG","Decoded #{@nb_line} lines for now ... ",inline: true) if @opts[:v] && @nb_line % 10000 == 0
            return true
        end
        ## return the hash of fields
        # key : field name
        # value : index in the record
        def get_fields code = nil
            fields = nil 
            ## ie. only one type of records, just fetch all fields directly
            if @RECORDS.empty?
                fields = @FIELDS
            else
                fields = @FIELDS[code]
            end
            Hash[fields.each_with_index.map { |v,i| [v.downcase.to_sym,i] }]
        end

    end


end