module Decoder

    class CSVDecoder
        require 'csv'
        require_relative '../debugger'
        include Decoder

        DEFAULT_SEP = ";"

        attr_accessor :sep

        def initialize opts = {}
            @opts = opts
            @sep = opts[:sep] || DEFAULT_SEP
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
        def decode file
            check file
            json = Hash.new { |h,k| h[k] = {} }
            ::CSV.foreach(file.full_path,@csv_opts) do |record|
                ## remove nils elements ...
                record = record.map { |c| c ?  c : "" }
                # type of record
                code = record.first.to_i
                name = @RECORDS.key(code)
                next unless name # if we have no idea what this records is ...

                # first time we see this record
                # so we take its fields name
                if !json[name][:fields]
                    json[name][:fields] = get_fields code
                    json[name][:values] = []
                end
                fields = json[name][:fields]
                json[name][:values] << record
            end
            sanitize_json json
            json = @mapper.map_json(json) if @mapper
            Debug::debug_json json if @opts[:d]
            json = @filter.filter_json(json) if @filter
        rescue => e
            Logger.<<(__FILE__,"ERROR","Error CSV Decoding file #{file.full_path}: #{e.message}")
            raise e
        end

        def get_fields code
            fields = Hash[@FIELDS[code].each_with_index.map { |v,i| [v.downcase.to_sym,i] }]
            fields
        end

    end


end
