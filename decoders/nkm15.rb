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
        require_relative '../debugger'

        DECODER = Conf::directories.app + "/decoding/newpmud.pl -NKM15RAW -F "
        DEFAULT_SEP = ":"
        TEST_FILE = "nkm15_test"
        DUMP_FILE = "nkm15_dump"
        
        def initialize(opts={})
            @opts = opts
            @separator = opts[:separator] || DEFAULT_SEP
        end
        
        ## decode a file and output a json =)
        def decode file,opts={}
            check file
            str = DECODER
            # not for now .. maybe find solution to avoid decoding all before 
            ##str += " --allowed=#{@records_allowed.join(',')} " unless @records_allowed.empty?
            str += "\"#{file.full_path}\""
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
                    name = fields.shift
                    fields = Hash[fields.each_with_index.map { |v,i| [v.downcase.to_sym,i] }]
                    rec =  { fields: fields, values: []} 
                    json[name] = rec
                    next
                end
                # finally , the rest is pur data ! only do not take the name
                # 'cause we dont use it
                values = line.split(":")
                rec[:values] << values
            end
            json = @mapper.map_json(json) if @mapper
            json = @filter.filter_json(json) if @filter
            Debug::debug_json(json) if @opts[:d]
            sanitize_json json
        end

    end

end
