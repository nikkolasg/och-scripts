module Decoder

    ## COntains all the fields for the CSV decoder
    class CSVFields

        def self.retrieve type
            f = type.upcase.to_s + "_FIELDS"
            r = type.upcase.to_s + "_RECORDS"
            f = self.const_get(f)
            r = self.const_get(r)
            raise "FIELDS decoder : type #{type} not corresponding to anything ..." unless (f && r)
            return [f,r]
        end
        ## TODO
        SMS_RECORDS = { "S" => 1 }
        fields = RubyUtil::symbolize([ "record_type",
                                       "submit_date",
                                       "reference_id",
                                       "a_number",
                                       "a_imsi",
                                       "b_number",
                                       "b_imsi",
                                       "tariff_class",
                                       "message_switch_id",
                                       "vmsc_addr"])

        SMS_FIELDS = { 1 => fields, 2 => fields}
        MMS_RECORDS = { "S" => 1, "R" => 2 }
        fields = RubyUtil::symbolize([ "record_type",
                                       "submit_date",
                                       "reference_id",
                                       "mmsc_id",
                                       "a_number",
                                       "a_imsi",
                                       "b_number",
                                       "b_imsi",
                                       "sgsn_owner",
                                       "tariff_class",
                                       "message_size" ])
        MMS_FIELDS = { 1 => fields,
                       2 => fields }

        MSS_RECORDS = { "MOC" => 1, "FORW" => 3, "POC" => 11 }
        MSS_FIELDS = { 1 => %w(
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

end
