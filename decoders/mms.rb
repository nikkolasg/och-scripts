module Decoder

    class TALDecoder
        include Decoder
        SEP = " "
        DUMP_FILE = "tal_dump"
        TEST_FILE = "tal_test"
        require_relative '../debugger'
        def initialize opts = {}
            @opts = opts
        end

        def decode file
            check file
            json = Hash.new { |h,k| h[k] = {} }
            IO.foreach(file.full_path) do |line|
                splitted = line.split(SEP)
                fields = {}
                values = []
                (0..splitted.size-1).each do |i|
                    if i % 2 == 1 ## VALUE
                        v = splitted[i].gsub(/"/,"")
                        f = FIELDS[splitted[i-1].to_i] ## works anyways
                        v = v == "NA" ? '' : v
                        values << v ## adding value
                        ind = values.size - 1
                        fields[f] = ind ## adding field with index
                    end
                end
                type = fields[:action] ## index 
                next unless type
                type = values[type]  ## type of cdr (Send, Receive etc)
                next unless type
                rec = json[type]
                unless rec[:fields] ## add fields to json
                    rec[:fields] = fields
                end
                rec[:values] = [] unless rec[:values]
                rec[:values] << values ## add values to json
            end
            json = @mapper.map_json(json) if @mapper
            json = @filter.filter_json(json) if @filter
            Debug::debug_json json if @opts[:d]
            return sanitize_json(json)
        end 

    ## 901 field code
    ACTIONS = { "S" => "Send",
                "R" => "Received",
                "G" => "Get",
                "N" => "Notification",
                "F" => "Forward",
                "J" => "Delete",
                "P" => "Deliver",
                "I" => "Store",
                "FD" => "FuturDelivery",
                "RS" => "MM4r",
                "M" => "MM4s"
    }           

    FIELDS = {
        602 => :recipient_push_status,
        603 => :message_key,
        617 => :message_rejected_indicator,
        618 => :cos_name,
        620 => :original_handset_type,
        628 => :message_class_size,
        901 => :action,
        902 => :message_type,
        903 => :message_content_type,
        904 => :charging_indicator,
        905 => :action_final_state, ## OPTIONAL
        906 => :client_type_bearer_type,
        907 => :mms_relay_id,
        908 => :message_size,
        909 => :mm_source_address,
        910 => :delivery_report_requested,
        911 => :read_reply_requested,
        912 => :indication_of_transcoding,
        913 => :mm7_service_code,
        914 => :reply_message_id,
        917 => :forward_message_indicator,
        918 => :time_of_expiry,
        919 => :earliest_time_of_delivery,
        920 => :error_status_code,
        921 => :error_detailed_status_code,
        922 => :reply_charging_requested,
        923 => :prepaid_type,
        924 => :charging_action,
        925 => :charge_rcpt,
        926 => :message_id,
        927 => :owner,
        928 => :entry_date,
        929 => :final_state_date,
        930 => :message_class,
        931 => :mm_destination_address,
        932 => :number_of_recipients,
        934 => :message_content_types_sizes,
        935 => :add_error_info,
        938 => :redistribution_indicator,
        939 => :message_rate,
        940 => :recipient_types,
        942 => :charged_party,
        944 => :handset_type,
        945 => :operator_id,
        946 => :imsi,
        947 => :sgsn,
        950 => :recipients_imsi,
        953 => :auto_provisionning_indication,
        954 => :vasp_id,
        955 => :gpp_mms_version,
        956 => :message_mm4_type,
        957 => :transaction_id,
        958 => :campaign_name,
        961 => :vas_id,
        981 => :vas_short_code,
        982 => :message_age,
        983 => :original_sender,
        984 => :message_creation_time,
        985 => :service_type,
        986 => :sender_type,
        987 => :campaign_id,
        988 => :number_of_notification_retries,
        991 => :maximal_number_of_retries,
        992 => :dr_status_extension,
        993 => :forward_dr_ua,
        995 => :application_id,
        996 => :reply_application_id,
        997 => :aux_application_info,
        998 => :mm7_charged_party_id
    }

end

end
