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
