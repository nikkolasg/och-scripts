module Mapper
    ## SMS MAPPER
    class LogicaMapper < GenericMapper

        def initialize opts = {}
            super(opts)
            @date = :submitDate
            @time = :submitTime
            @nfield = :submit_date ##new field combined
            p = Proc.new do |field,nf|
                { "#{field}.TON" => "#{nf}_ton",
                  "#{field}.NPI" => "#{nf}_npi",
                  "#{field}.PID" => "#{nf}_pid",
                  "#{field}.MSISDN" => "#{nf}_number" }
            end
            @fields2change = { "a_number" => "a","b_number" => "b",
                               "called_VMSC_no" => "called_vmsc_no",
                               "calling_VMSC_no" => "calling_vmsc_no",
                               "suscriber_a_number" => "subscriber_a" }
            @fields2change = @fields2change.inject({}) { |col,(of,nf)| 
                col.merge!(p.call(of,nf));
                col }
            @fields2change = RubyUtil::symbolize(@fields2change,values: true)
            @fields2change[:lengthOfMessage] = :message_size
            @fields2change[:origIntlMobileSubId] = :a_imsi
            @fields2change[:intlMobileSubId] = :b_imsi
            @fields2change[:messageReference] = :reference_id
            @fields2change.merge! map_time_fields([:submit_date])
        end

        def map_json json
            json.each do |name,hash|
                fields = hash[:fields]
                ## Reassemble Date + time
                idate = fields[@date]
                itime = fields[@time]
                unless idate && itime 
                    Logger.<<(__FILE__,"ERROR","While Mapper Logica json. No date /time field found ! #{fields}")
                    abort
                end
                fields.delete(@date)
                fields.delete(@time)
                fields[@nfield] = idate ## replace old date by new one
                ## Dessasemble billid into tariffclass + sid
                ## for simplicity we will use the remaining index itime
                # for placing the sid values
                i = fields.delete(:billid)
                fields[:tariff_class] = i
                fields[:sid] = itime
                hash[:values].each do |row|
                    ## TIME CHANGE
                    t = row[itime].gsub(":","")
                    d = "20"+row[idate].gsub("/","")
                    row[idate] = d+t
                    transform_time_values row,idate
                    ## BILLID CHANGE ==>
                    ## into tariff_class + sid
                    v = row[i]
                    row[itime] = '' if v.empty?
                    if v.index("#")
                        ## looks like #tarrif#sid
                        ### #45789#189
                        a = v.split("#")
                        row[i] = a[1]
                        row[itime] = a[2]
                    else
                        row[i] = v
                    end
                end
                hash[:fields] = rename_fields fields, @fields2change
            end
            json
        end

    end

    ## sms out mapper
    class SmsMapper < GenericMapper

        ## basically rename vmsc_addr to calling and create a new entry called vmsc number 
        #that way both input and ouput are normalized
        def initialize opts = {}
            super(opts)
            @fields2change = { record_type: :status,vmsc_addr: :calling_vmsc_no_number }
            @fields2change.merge! map_time_fields([:submit_date])
        end

        def map_json json
            json.each do |name,hash|
                fields = hash[:fields]
                values = hash[:values]
                icalling = fields[:vmsc_addr]
                icalled = values.first.length
                fields[:called_vmsc_no_number] = icalled
                fields[:a_pid] = icalled + 1
                values.each do |row|
                    transform_time_values row,fields[:submit_date]
                    transform_status row,fields[:record_type] 
                    transform_vmsc row,fields
                    add_pid row,fields
                end
                rename_fields fields,@fields2change 
            end
            json
        end 

        def transform_status  row,index
            v = row[index]
            return  unless v
            v = "S" if v.to_i == 1
            v = "R" if v.to_i == 2
            row[index] = v
        end

        def transform_vmsc row,fields
            row << "" ## add called at the end 
            ## if only it is MT, we place into the vmsc CALLED addr
            if row[fields[:record_type]] == "R"
                row[fields[:called_vmsc_no_number]] = row[fields[:vmsc_addr]]
                row[fields[:vmsc_addr]] = ""
            end
        end

        def add_pid row,fields
            row << "plmn"
        end

    end

end
