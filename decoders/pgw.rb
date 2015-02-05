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
