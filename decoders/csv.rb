module Decoder

    class CSVDecoder
        require 'csv'
        include Decoder

        DEFAULT_SEP = ";"

        attr_accessor :sep

        def initialize opts = {}
            @opts = opts
            @sep = opts[:sep] || DEFAULT_SEP
            ## take all records
            self.records_allowed = RECORDS.keys
        end

        def decode file
            check file
            json = Hash.new { |h,k| h[k] = {} }
            ::CSV.foreach(file.full_path,{ col_sep: @sep }) do |row|
                # type of record
                code = row.shift.to_i
                next unless @records.key? code
                name = RECORDS.key(code)

                indexes = @records[code]

                # first time we see this record
                # so we take its fields name
                if !json[name][:fields]
                    if !indexes.empty? # we have specified indexes
                        json[name][:fields] = FIELDS[code].values_at(*indexes)
                    else # we take it all
                        json[name][:fields] = FIELDS[code]
                    end
                    json[name][:values] = []
                end

                if !indexes.empty? # take specific values
                    values = row.values_at(*indexes)
                else # take it all
                    values = row
                end
                json[name][:values] << values
            end
            json
        rescue => e
            Logger.<<(__FILE__,"ERROR","Error CSV Decoding file #{file.full_path}: #{e.message}")
            raise e
        end

        ## look at the records definition to pick up the record type number,
        #and the fieds associated 
        def records_allowed=(records)
            @records = Hash.new 
            records.each do |record_type|
                code = RECORDS[record_type]
                next unless code ## Suppose to be a mistake ... !
                @records[code] = []
            end
        end
        def records_allowed
            RECORDS.invert.values_at(*@records.keys)
        end

        def fields_allowed
            @records.map { |code,indexes| FIELDS[code].values_at(*indexes) }.flatten(1)
        end

        ## filter out the fields
        def fields_allowed=(fields)
            @records.keys.each do |code|
                # take the index of the fields 
                # given that they exists in the FIELDS
                fields.each do |f| 
                    i = FIELDS[code].index(f)
                    @records[code] << i if i 
                end
            end
        end
    end


end
