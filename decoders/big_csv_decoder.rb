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
