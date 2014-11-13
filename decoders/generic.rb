module Decoder

    # klass_name is the name of the class to instantiate in the module
    # records is a list a records to keep for the decoder. Others will not be decoded
    # fields is a list of fields to keep for the decoder
    def self.create klass_name,records = nil,fields = nil,opts = {}
        if klass_name.is_a?(String)
            klass_name = klass_name.capitalize.to_sym
        end
        decoder = Decoder::const_get(klass_name).new(opts)
        decoder.records_allowed = records if records
        decoder.fields_allowed = fields if fields
        decoder
    end

    # generic functions that are sueful to classes implementing a decoder
    # + method "Inferface" a la JAVA
    attr_accessor :records_allowed,:fields_allowed 

    def decode(file)
        raise "NotImplementedError"
    end

    # check if file exists or raise error
    def check file
        unless ::File.exists? file.full_path
            Logger.<<(__FILE__,"ERROR","Decoder::decode File does not exists #{file.full_path}")
            abort
        end
        if file.zip?
            res =file.unzip!
            unless res
                Logger.<<(__FILE__,"ERROR","Decoder:: unzip file error #{file.name}")
                return false
            end
        end
        return true
    end

    ## execute the given command and sends back the results
    def exec_cmd command
        error, out = nil
        Open3.popen3(command) do |sin,sout,serr,thr|
            sin.close
            out = sout.read
            error = !thr.value.success?
        end
        if error
            Logger.<<(__FILE__,"ERROR","Decoder::exec_cmd error while executing")
            abort
        end
        return out
    end

    # filtering method where you give your list of fields
    # and it sends back the filtered list of fields + indexes
    # where to find them in the raw data (index 4,6,18,20 ...) 
    def filter fields
        new_fields = []
        indexes = []
        fields.each_with_index do |value,index|
            next unless @fields_allowed.include? value
            new_fields << value
            indexes << index
        end
        return new_fields,indexes
    end

    def self.json_stats json
        json.each do |k,v|
            puts "#{k} => #{v[:values].size} entries. #{v[:fields].size} fields vs #{Util::array_avg(v[:values])} values avg"
        end 
    end

    #def to_s
        #puts "Decoder #{self.class} : records allowed(#{self.records_allowed.class}) => #{self.records_allowed}"
        #puts "Decoder #{self.class} : fields allowed(#{self.fields_allowed.class}) size#{self.fields_allowed.size} => #{self.fields_allowed}" 
    #end

    def self.test_decoder(klass_name)
        instance = create(klass_name)
        folder = File::dirname(__FILE__)
        file = klass_name.to_s.gsub("Decoder","").downcase
        file =  folder + "/" + file
        file = CDR::File.new(file,search:true)
        file.unzip! if file.zip?
        json = instance.decode(file,v: true)
        self.json_stats json
    end


    RECORDS = { "MOC" => 1, "FORW" => 3, "POC" => 11 }
    FIELDS = { 1 => %w(
                record_type
                record_number
                call_reference
                gt
                intermediate_record_number
                intermediate_charging_ind
                number_of_ss_records
                calling_imsi
                calling_imei
                calling_number
                calling_category
                calling_ms_classmark
                called_imsi
                called_imei
                dialled_digits_ton
                called_number
                called_category
                called_ms_classmark
                dialled_digits
                calling_subs_first_lac
                calling_subs_first_ci
                calling_subs_last_ex_id
                charcalling_subs_last_lac
                calling_subs_last_ci
                called_subs_first_lac
                called_subs_first_ci
                called_subs_last_ex_id
                called_subs_last_lac
                called_subs_last_ci
                out_circuit_group
                out_circuit
                basic_service_type
                basic_service_code
                non_transparency_indicator
                channel_rate_indicator
                set_up_start_time
                in_channel_allocated_time
                charging_start_time
                charging_end_time
                orig_mcz_duration_ten_ms
                cause_for_termination
                data_volume
                call_type
                orig_mcz_tariff_class
                orig_mcz_pulses
                dtmf_indicator
                aoc_indicator
                called_number_ton
                facility_usage
                pni
                orig_mcz_chrg_type
                called_msrn_ton
                called_msrn
                calling_charging_area
                called_charging_area
                calling_number_ton
                cug_interlock
                cug_outgoing_access
                regional_subs_indicator
                regional_subs_location_type
                intermediate_chrg_cause
                cug_information
                calling_modify_parameters
                orig_mcz_modify_percent
                orig_mcz_modify_direction
                orig_dialling_class
                orig_mcz_change_percent
                orig_mcz_change_direction
                number_of_in_records
                scp_connection
                leg_call_reference
                routing_category
                speech_version
                add_routing_category
                call_reference_time
                camel_call_reference
                camel_exchange_id_ton
                camel_exchange_id
                number_of_all_in_records),
                    3 => %w(
                record_type
                record_number
                call_reference
                intermediate_record_number
                intermediate_charging_ind
                number_of_ss_records
                cause_for_forwarding
                forwarding_imsi
                forwarding_imei
                forwarding_number
                forwarding_category
                forwarding_ms_classmark
                forwarded_to_imsi
                forwarded_to_imei
                forwarded_to_number
                forwarded_to_ms_classmark
                orig_calling_number
                in_circuit_group
                in_circuit
                forwarding_first_lac
                forwarding_first_ci
                forwarding_last_ex_id
                forwarding_last_lac
                forwarding_last_ci
                forwarded_to_first_lac
                forwarded_to_first_ci
                forwarded_to_last_ex_id
                forwarded_to_last_lac
                forwarded_to_last_ci
                out_circuit_group
                out_circuit
                basic_service_type
                basic_service_code
                non_transparency_indicator
                channel_rate_indicator
                set_up_start_time
                in_channel_allocated_time
                charging_start_time
                charging_end_time
                forw_mcz_duration
                cause_for_termination
                data_volume
                call_type
                forw_mcz_tariff_class
                forw_mcz_pulses
                dtmf_indicator
                aoc_indicator
                forwarded_to_number_ton
                facility_usage
                pni
                forw_mcz_chrg_type
                forwarded_to_msrn_ton
                forwarded_to_msrn
                forwarding_charging_area
                forwarded_to_charging_area
                forwarding_number_ton
                orig_calling_number_ton
                intermediate_chrg_cause
                orig_dialling_class
                forw_mcz_change_percent
                forw_mcz_change_direction
                number_of_in_records
                scp_connection
                leg_call_reference
                routing_category
                speech_version
                add_routing_category
                call_reference_time
                camel_call_reference
                camel_exchange_id_ton
                camel_exchange_id
                number_of_all_in_records
                loc_routing_number_ton
                loc_routing_number
                npdb_query_status),
                    11 => %w(
                record_type
                record_number
                call_reference
                network_id
                intermediate_record_number
                intermediate_charging_ind
                number_of_ss_records
                calling_number_ton
                calling_number
                called_number_ton
                called_number
                in_circuit_group
                in_circuit
                basic_service_type
                basic_service_code
                in_channel_allocated_time
                charging_start_time
                charging_end_time
                cause_for_termination
                call_type
                ticket_type
                iaz_chrg_type
                iaz_duration
                iaz_tariff_class
                iaz_pulses
                leg_call_reference
                called_msrn_ton
                called_msrn
                intermediate_chrg_cause
                orig_dialling_class
                camel_call_reference
                loc_routing_number_ton
                loc_routing_number
                npdb_query_status)
    }


end
