module Decoder

    class XDRDecoder

        include Decoder

        def initialize opts = {}
            @opts = opts
            @header_lines = 3 ## lines at the beginning to skip
            @footer_lines = 1 ## lines at the end to skip
            @type = "XDR"
        end

        def dump_file
            @dump_file ||= File.dirname(__FILE__) +"/"+ "xdr_dump"
        end

        def test_file
            @test_file ||= File.dirname(__FILE__) +"/"+ "xdr_test"
        end


        # main method
        def decode file,&block
            check file

            ## prepare the fields 
            @fields_range, @fields_index = filter_fields
            @json = { @type =>  { :fields => @fields_index, values: [] }}
            stop_line = file_number_line(file) - @footer_lines ## line where to stop

            File.open(file.full_path,"r") do |fhandler|
                lines = fhandler.lines
                @header_lines.times { lines.next }  ## skip header
                @current_line = @header_lines 
                lines.each do |line|
                    (puts "Stop at line no #{@current_line} : #{line}";break;) if @current_line >= stop_line 
                    SignalHandler.check { Logger.<<(__FILE__,"WARNING","Stopping decoding Ctrl-c catched"); fhandler.close }
                    analyze line.chomp, &block
                end
            end
            sanitize_json @json
            Debug::debug_json @json if @opts[:d]
            @mapper.map_json @json if @mapper
            @filter.filter_json @json if @filter
            Debug::debug_json @json if @opts[:d]
            @json
        rescue => e
            Logger.<<(__FILE__,"ERROR","Error XDR Decoding #{file.full_path}")
            raise e if @opts[:d]
            return @json
        end

        ## decode the line and store it into the json
        def analyze line,&block
            record = []
            ## insert the value into the record
            @fields_range.each do |field,range|
                record.push((line[range] || "").strip)
            end

            @json[@type][:values] << record
        end

    ## Directly map the fields 
    # Return the fields with associated range in the line
    # AND fields with associated index in the record JSON structure (as normal)
    def filter_fields
        @fields_by_range = FIELDS
        @fields_by_index = @fields_by_range.each_with_index.inject({}) do |col,((k,v),i)|
            col[k] = i
            col
        end
        return @fields_by_range,@fields_by_index
    end

    ## Fields used by the XDR file format
    #  name of field => range of char. in line (start at index 1)
    FIELDS = {
        :record_type    => (0..1),
        :cdp_id         => (3..7),
        :network_type   => 8,
        :a_number       => (9..23),
        :corr_id        => (24..35),
        :modif_ind      => 40,
        :type_of_numb   => 41,
        :numbering_plan => 42,
        :called_number  => (43..63),
        :service_type   => 64,
        :service_code   => (65..66),
        :text_id        =>  (67..71),
        :switch_id      => (88..102),
        :event_id       => (103..104),
        :whole_sale_perc => (108..112),
        :sign           => 113,
        :charging_date  => 114..119,
        :charge_time    => 120..125,
        :utf_offset     => 126,
        :chargeable_unit    =>  127..132,
        :data_vol_ref   => 133..138,
        :retail_charge  => 139..147,
        :whole_sale_charge  => 148..156,
        :interconnect_charge    => 157..165,
        :revenue_share_charge   => 166..174,
        :tax_rate_code  => 176,
        :exchange_rate_code => 177,
        :tariff_class   => 178..183,
        :seq_indicator  => 184,
        :seq_number     => 185..186,
        :mvne_id        => 187..192,
        :mvno_id        => 193..198,
        :pop_id         => 199..204,
        :charging_imsi  => 205..220,
        :switch_name    => 221..227,
        :circuit_group  => 228..232
    }
end

end
