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
    require './config/config'
    require 'open3'
    require './util'
    # helper class to facilitate the 
    # different format of cdr, and compressed or not ,
    # stored in subfolder ( ==> cname) etc.
    class File
        ## cname is the "real"  name
        #  name is just the name of the cdr, without any others extensions
        attr_accessor :name,:cname,:full_path

        def initialize(name,path,opts = {})
            if opts[:search]
                find_itself name,path
            else
                @name = remove_ext name
                @path = path
                @cname = name
                @full_path = path + "/" + name
            end
            @flow = Util::flow name, return: :class
        end

        # custom writer so it updates relative attributes also
        def path= param 
            @path = param
            @full_path = @path + "/" + @cname
        end
        def path
            @path
        end

        def ==(other)
            return true if @cname == other.cname && @path == other.path
            return false
        end
        alias :eql? :== 
        def hash
            @cname.hash + @path.hash
        end

        # return true if the file is a zipped file
        def zip?
            find_itself unless @cname
            if @cname.end_with?(".GZ") || @cname.end_with?(".gz")
                return true
            end
            false
        end

        ## compress the file and change inner variables
        def zip!
            find_itself unless @full_path
            cmd = "gzip -f #{@full_path}"
            unless system(cmd)
                Logger.<<(__FILE__,"ERROR","CDR::File could not zip itself (#{@cname}). Abort.");
                abort
            end
            @cname = @name 
            @full_path = @path + "/" + @cname       
        end


        def decode! opts = {}
            find_itself unless @cname
            unless ::File.exists?(@full_path)
                Logger.<<(__FILE__,"ERROR","CDR::File does not exists #{@cname}")
                abort
            end
            unless @flow.decoder
                Logger.<<(__FILE__,"ERROR","CDR::File flow #{@flow} does not have any decoder.")
                abort
            end
            unzip if zip?
            meth = "decode_#{@flow.name.downcase}"
            CDR::send(meth.to_sym,@full_path,opts)
        end
        def to_s
            "CDR::File #{@name} (exists = #{::File.exists?(@path + "/" +@cname)}) Compressed : #{zip?}"
        end

        private

        # will try to find the value of cname
        # searching in the directory
        def find_itself name,path
            @full_path = nil
            f = path + "/" + name
            ["",".gz",".GZ"].each do |ext|
                f_ = f + ext
                if ::File.exists? f_
                    @full_path = f_
                    break
                end
            end
            (Logger.<<(__FILE__,"ERROR","CDR:File cannot find it self ... #{path}/#{name}");abort;) unless @full_path
            @cname = ::File.basename(@full_path)
            @name = name
            @path = path
        end
        # remove the extension pf the name
        def remove_ext name
            ::File.basename(name.gsub(/(\.GZ|\.gz)/,""))
        end

        def unzip
            cmd = "gunzip #{@full_path}"
            unless system(cmd)
                Logger.<<(__FILE__,"ERROR","CDR::File uncompress error ...")
            end
            @cname = @name
            @full_path = @path + "/" + @cname
        end

    end

    # decode a MSS file data
    def self.decode_mss file,opts = {}
        flow = App.flow(:MSS)
        decoder_cmd = flow.decoder + " "      
        if opts[:allowed]
            decoder_cmd += " --allowed=#{flow.records_allowed.join(',')} "
        end
        decoder_cmd += file
        if opts[:out]
            decoder_cmd += " > #{opts[:out]}"
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
        return convert_json_mss out, opts
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
        f2keep = App.flow(:MSS).records_fields.keys 
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



    ## create a file used by create_tables
    #from a decoded output with the format
    #field:MysqlType
    #used to create multiple table easily directly from
    #the output of the decoder
    #snend this file to create_table after
    def self.dump_table_file json,file_name
        open(file_name,"w") do |file|
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
