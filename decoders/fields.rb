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
        ## RECORDS : used to describe the different records type inside a file
        # key : name
        # value : value to expect to find in the first field of the file
        # For only files with ONE RECORD TYPE :
        # create an empty HASH, so that the decoder knows it hash nothing to look for

               TAP_RECORDS = { }
        TAP_FIELDS = RubyUtil::symbolize( %w( event_type
                                              sender
                                              recipient
                                              file_seq_num
                                              mm_call_case_num
                                              start_date
                                              start_time
                                              start_utc_offset
                                              duration
                                              charged_party
                                              orig_address
                                              orig_imsi
                                              orig_imei
                                              dest_address
                                              dest_imsi
                                              dest_imei
                                              translated_address
                                              dialled_digits
                                              msc_address
                                              call_identification
                                              record_id
                                              first_calling_location
                                              first_called_location
                                              partial_indicator
                                              cause_for_record_closing
                                              tele_service_code
                                              bearer_service_code
                                              supp_service_code
                                              action_code
                                              camel_service_key
                                              camel_service_level
                                              camel_default_call_handling_ind
                                              service_centre_address
                                              third_party_number
                                              sms_orig_subscriber
                                              sms_termi_subscriber
                                              ggsn_address
                                              sgsn_address
                                              sgsn_plmn_id
                                              access_point_name_ni
                                              access_point_name_oi
                                              pdp_type
                                              served_pdp_address
                                              qos_negotiated
                                              data_incoming
                                              data_outgoing
                                              charge_local_curr
                                              charged_item
                                              exchange_rate
                                              call_type_level1_1
                                              call_type_level2_1
                                              call_type_level3_1
                                              charge
                                              chargeable_unit
                                              charged_unit
                                              tax_value
                                              charged_item_2
ExchangeRate_2
CallTypeLevel1_2
CallTypeLevel2_2
CallTypeLevel3_2
Charge_2
ChargeableUnits_2
ChargedUnits_2
TaxValue_2
charged_item_3
ExchangeRate_3
CallTypeLevel1_3
CallTypeLevel2_3
CallTypeLevel3_3
Charge_3
ChargeableUnits_3
ChargedUnits_3
TaxValue_3
charged_item_4
ExchangeRate_4
CallTypeLevel1_4
CallTypeLevel2_4
CallTypeLevel3_4
Charge_4
ChargeableUnits_4
ChargedUnits_4
TaxValue_4
tap_decimal_places
exchange_rate_decimal_places
local_currency))


        TAP_DWH_RECORDS = {}
        TAP_DWH_FIELDS = TAP_FIELDS




        PROCERA_RECORDS = {}
        ## FIELDS : used to describe the different fields name for the file (in order)
        PROCERA_FIELDS = RubyUtil::symbolize( %w( a_number
                                                  handset
                                                  ip
                                                  start_time
                                                  end_time
                                                  empty_field
                                                  protocol_category
                                                  protocol_name
                                                  ap_name
                                                  browser
                                                  os
                                                  tethered_state
                                                  vol_uplink
                                                  vol_downlink
                                                  total ) )


        PGW_RECORDS = {}
        PGW_FIELDS = RubyUtil::symbolize( %w(   a_number
                                                a_imsi
                                                start_time
                                                duration
                                                ap_name
                                                rating_category
                                                sgsn_owner
                                                vol_uplink
                                                vol_downlink
                                                mcc_mnc ) )

        SMS_RECORDS = { "S" => 1, "R" => 2 }
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