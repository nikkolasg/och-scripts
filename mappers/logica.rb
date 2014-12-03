module Mapper

    ## Simply concatenate time and date field
    class LogicaMapper < GenericMapper

        def initialize opts = {}
            super(opts)
            @date = :submitDate
            @time = :submitTime
            @nfield = :time ##new field combined
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
        end

        def map_json json
            json.each do |name,hash|
                fields = hash[:fields]
                idate = fields[@date]
                itime = fields[@time]
                unless idate && itime 
                    Logger.<<(__FILE__,"ERROR","While Mapper Logica json. No date /time field found ! #{fields}")
                    abort
                end
                fields.delete(@date)
                fields.delete(@time)
                hash[:values].each do |row|
                    ## TIME CHANGE
                    t = row[itime].gsub(":","")
                    d = "20"+row[idate].gsub("/","")
                    row[idate] = d+t
                end
                fields[@nfield] = idate ## replace old date by new one
                hash[:fields] = rename_fields fields, @fields2change
            end
            json
        end

    end

    class SmsMapper < GenericMapper

        def initialize opts = {}
            super(opts)

        end

        def map_json json
            json.each do |name,hash|
                fields = hash[:fields]
                values = hash[:values]
                fields = rename_fields fields, { :record_type => :status }
                hash[:fields] = transform_status fields,values 
            end
        end 
        def transform_status fields, values
            i = fields[:status]
            values.each do |row|
                v = row[i]
                next unless v
                v = "S" if v.to_i == 1
                v = "R" if v.to_i == 2
                row[i] = v
            end
        end

    end

end
