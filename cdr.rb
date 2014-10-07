#!/usr/bin/ruby
#
#MODULE DECODER
# got one decode method per flow
# OUTPUT
# filename : {
#       k1 : v1,
#       k2 : v2,
#       ...
# }
# MAYBE switch to a more metaprogramming approach
# where you specify whhat you want to do at launch time
# like 
# decoder :MSS do
#   cmd "newpmud.pl ....."
#   res.map { |k,v| ... }
#end
module CDR
require './config'
require 'open3'

    def self.decode file, opts = {}
        unless opts[:flow]
            raise "CDR Module:decode() No flow specified"
        end

        unless File.exists?(file)
            raise "CDR Module:decode() File does not exists #{file}"
        end

        flow = opts[:flow] # to change after, guess automatically from name
        unless EMMConfig["#{flow.to_s.upcase}_DECODER"]
            raise "CDR Module:decode() Type has no decoder specified #{flow}"
        end

        case flow
            when :MSS
                out = decode_mss(file,opts)
                json = convert_json(out,:MSS,opts) if !opts[:intact] # do not specify to get default json output
            else
                raise "CDR Module:decode() Type is not implemented"
        end

    end
    
    # decode a MSS file data
    def self.decode_mss file,opts = {}
        decoder_cmd = EMMConfig["MSS_DECODER"].dup        
        if opts[:allowed]
            decoder_cmd << " --allowed=#{opts[:allowed]} "
        end
        decoder_cmd << file
        if opts[:out]
            decoder_cmd << " > #{opts[:out]}"
        end
        error = nil
        out = nil
        Open3.popen3(decoder_cmd) do |sin,sout,serr,thr|
            sin.close
            out = sout.read

            error = !thr.value.success?
        end
        if error
            raise "CDR Module:decode_mss() error while executing #{decoder_cmd}"
        end
        out
    end

    def self.convert_json raw,flow,opts = {}
        return unless raw
        out = []
        if flow == :MSS
            out = convert_json_mss raw, opts
        end
        out
    end
    
    def self.convert_json_mss raw, opts = {}
            out = []
            rec = nil # current record flow we are examinating
            indexes = nil # indexes to keep if filtering enabled
            raw.split("\n").each do |line|
                # line starting with ### are delimieter between different record flow
                if line.start_with? "###"
                    out << rec
                    rec = nil
                    indexes = nil
                    next
                end
                ## no record selected yet, so there should be the definition of one 
                if rec == nil
                    fields = line.split(":")
                    name = fields.shift # first value is the name of the record flow
                    
                    fields,indexes = filter(:MSS,fields) if opts[:filter]   
                    
                    rec =  { name: name, fields: fields, values: []} 
                    next
                end
                # finally , the rest is pur data !
                values = line.split(":")
                # filter values for selected fields
                values = values.values_at(*indexes) if (opts[:filter] && indexes)

                rec[:values] << values
            end
            out
    end

    # filter out the fields for the particular record
    # flow of CDR
    # fields to filter
    # RETURN [fields,indexes]
    # where fields is an array of filtered field
    #       indexes is an array of indexes to filter for the data
    def self.filter flow,fields
        f2keep = RubyUtil::arrayize EMMConfig["#{flow}_RECORDS_FIELDS"]
        newFields = []
        indexes = []
        fields.each_with_index do |value,index|
            alors =  f2keep.include? value
            next unless alors
            newFields << value
            indexes << index
        end
        return newFields,indexes
    end
    ## test function 
    # to launch when moving arch, or go to prod
    # TO UPGRADE so it can fetch a file alone by flow etcetc
    def self.test_decode opts = {}
        if opts[:file]
            file = opts[:file]
            puts "File specified : #{file}"
        else
            file = EMMConfig["MSS_TEST"]
            puts "No file specified, switch to config file ... #{file}"
        end

        unless File.exists? file
            $stderr.puts "File does not exists"        
            abort
        end
        unless opts[:flow]
            $stderr.puts "No flow specified"
            abort
        end
        # MSS TEST
        begin
            out = decode_mss(file,opts)
            json = convert_json(out,:MSS)
            dump_table_file json, :MSS
        rescue => e
            $stderr.puts e.message + "\n" + e.backtrace.join("\n")
            abort
        end
    end


    ## create a file used by create_tables
    #from a decoded output with the format
    #field:MysqlType
    #used to create multiple table easily directly from
    #the output of the decoder
    #snend this file to create_table after
    def self.dump_table_file json,flow
       open("fields_#{flow.downcase}_records.db","w") do |file|
            json.each do |record|
                values = record[:values].first
                record[:fields].each_with_index do |field,index|
                   str = field + ":"
                   str << "CHAR(#{values[index].length})"
                   file.puts str
                end
            end
       end
    end
end
