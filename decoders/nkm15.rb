module Decoder
    ## Implementation of 2 decoders for the MSS flow
    #simply override the methods decode. This method must
    #return a JSON in format
    #key = name of records
    #value = HASH : fields => arr of fields name
    #               values => matrix of values
    class NKM15Decoder
        require 'open3'
        include Decoder

        DECODER = "decoding/newpmud.pl -NKM15RAW -F "

        def initialize(opts={})
            @opts = opts
            @records_allowed = []
            @fields_allowed = []
        end
        def decode file,opts={}
            check file
            str = DECODER
            str += " --allowed=#{@records_allowed.join(',')} " unless @records_allowed.empty?
            str += file.full_path
            puts "Command : #{str}" if opts[:v]
            out = exec_cmd(str) 
            out2json(out)
        end

        # transform the decoder output into generic format json
        def out2json(out)   
            json = Hash.new { |h,k| h[k] ={} }
            rec = nil # current record flow we are examinating
            indexes = nil # indexes to keep if filtering enabled
            out.split("\n").each do |line|
                # line starting with ### are delimieter between different record flo
                if line.start_with? "###"
                    rec = nil
                    indexes = nil
                    next
                end
                ## no record selected yet, so there should be the definition of one 
                if rec == nil
                    fields = line.split(":")
                    name = fields.shift # first value is the name of the record flow
                    fields,indexes = filter(fields) unless @fields_allowed.empty?
                    rec =  { fields: fields, values: []} 
                    json[name] = rec
                    next
                end
                # finally , the rest is pur data !
                values = line.split(":")
                # filter values for selected fields
                values = values.values_at(*indexes) if (!@fields_allowed.empty? && indexes)
                rec[:values] << values
            end
            json
        end
    end

end
