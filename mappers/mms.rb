module Mapper
    class MmsInMapper < GenericMapper

        def initialize opts = {}
            super(opts)
            @time_fields = [:entry_date,:final_state_date]
            @id = :message_id
            @numbers = {:mm_source_address => [:a_number,:a_mail],
                        :mm_destination_address => [:b_number,:b_mail]}
            @imsi = { :imsi => :a_imsi, :recipients_imsi => :b_imsi }
        end

        def map_json json
            json.each do |name,hash|

                fields = hash[:fields]
                values = hash[:values]
                osize = values.first.size
                # transform rows ...
                values.each do |row|
                    transform_numbers_row fields,row
                    transform_time fields,row
                    transform_id fields,row
                end
                ## changes the fields after all (becase index=))
                transform_numbers_field fields,osize
                rename_fields fields,@imsi
            end
            json
        end

        private 


        # transform the fields number into two
        # add the resulting to the END of the row
        # row_size is the size of the row before
        # so new indexes start from this
        #  CALL AT THE END =)
        def transform_numbers_field fields,row_size
            count = 0
            @numbers.each do |n,arr|
                i = fields.delete(n) # delete the old field
                ## replace by the new fields instead
                arr.each { |v| fields[v] = row_size + count; count += 1 }
            end
            return fields
        end

        def transform_numbers_row fields,row
            re = /(\+?\d*)?(\/TYPE=PLMN@?)?([a-z0-9\._-]*)@?([a-z0-9\._-]*)?/i
            @numbers.each do |f,arr|
                i = fields[f]
                next unless i
                val = row[i]
                re.match val
                number = $1 || ""
                mail = ""
                mail += $3 if $3
                mail += "@"+$4 if $3 && !$3.empty? && $4 && !$4.empty?
                row << number.sub("+","")
                row << mail
            end
            return row
        end

        ## Remove trailing xxx+01 at the end of time field
        def transform_time fields,row
            @time_fields.each do |f|
                v = fields[f]
                next unless v
                ## change time field so it looks like standard
                #   Y   Mon Day H   Mn  Sec
                n = 4 + 2 + 2 + 2 + 2 + 2   - 1 # arr start at 0 =)
                row[v] = row[v].gsub("+01","")[0..n]
            end
            return row
        end
        ## remove address part of the id 
        def transform_id fields,row
            return row unless fields[@id]
            i = fields[@id]
            /(?<id>\w+)@.*/ =~ row[i]
            row[i] = id if id
        return row
        end
    end

    class MmsOutMapper < GenericMapper
        TRANSFORMS = { :record_type => :action,
                       :submit_date => :entry_date,
                       :reference_id => :message_id }

        def initialize(opts = {})
            super(opts)
        end

        def map_json json
            json.each do |name,hash|
                fields = hash[:fields]
                values = hash[:values]
                rename_fields fields,TRANSFORMS
                transform_action(fields,values)
            end
        end
        def transform_action fields , values
            ind = fields[:action]
            values.each do |row|
                v = row[ind]
                if v.to_i == 1
                    v = "S"
                elsif v.to_i == 2
                    v = "R"
                end
                row[ind] = v
            end
        end
    end
end

