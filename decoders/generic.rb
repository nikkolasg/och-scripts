module Decoder
    require_relative 'dump'
    # klass_name is the name of the class to instantiate in the module
    # records is a list a records to keep for the decoder. Others will not be decoded
    # fields is a list of fields to keep for the decoder
    def self.create klass_name,opts = {}
        if klass_name.is_a?(String)
            klass_name = klass_name.capitalize.to_sym
        end
        decoder = Decoder::const_get(klass_name).new(opts)
        decoder
    end

    attr_accessor :mapper,:filter,:opts
   
    def decode(file)
        raise "NotImplementedError"
    end
    def dump_file
        dump_path = File.dirname(__FILE__) + "/" + self.class::DUMP_FILE
    end
    def test_file
        test_path = File.dirname(__FILE__) + "/" + self.class::TEST_FILE
    end
    
    ## Remove all empty entries
    #  Like those rows we do not want, but there's still an entry for it
    def sanitize_json json
        json.delete_if do |name,hash|
            # do Not delete if there is one which is Not empty
            !hash[:values].any? { |row| !row.empty? }
        end
        return json
    end

    ## Return fields hash 
    # key : field name
    # value : sql
    # # WARNING : DO NOT DECLARE @fields VARIABLE IN ANY DECODER
    # otherwise the dumping wont take place since assign it one time
    def fields
        ## First time we have to create the dump file (i.e. the fields + sql)
        if !File.exists?(dump_file)
            Logger.<<(__FILE__,"INFO", "DECODER:GENERIC will decode & dump #{test_file}")
            @fields ||= MysqlDumper::dump self,[test_file],file: dump_file
        else # otehrwise directly read the dump file !
            @fields ||= MysqlDumper::read_dump_file dump_file
        end
        return @fields
    end


    # check if file exists or raise error
    def check file
        unless ::File.exists? file.full_path
            Logger.<<(__FILE__,"ERROR","Decoder::decode File does not exists #{file.full_path}")
            abort
        end
        if file.zip?
            res =file.unzip!
            unless res
                Logger.<<(__FILE__,"ERROR","Decoder:: unzip file error #{file.name}")
                return false
            end
        end
        return true
    end

    ## execute the given command and sends back the results
    def exec_cmd command
        error, out = nil
        Open3.popen3(command) do |sin,sout,serr,thr|
            sin.close
            out = sout.read
            error = !thr.value.success?
        end
        if error
            Logger.<<(__FILE__,"ERROR","Decoder::exec_cmd error while executing")
            abort
        end
        return out
    end

    # filtering method where you give your list of fields
    # and it sends back the filtered list of fields + indexes
    # where to find them in the raw data (index 4,6,18,20 ...) 
    def filter fields
        new_fields = []
        indexes = []
        fields.each_with_index do |value,index|
            next unless @fields_allowed.include? value
            new_fields << value
            indexes << index
        end
        return new_fields,indexes
    end

    def self.json_stats json
        json.each do |k,v|
            puts "#{k} => #{v[:values].size} entries. #{v[:fields].size} fields vs #{Util::array_avg(v[:values])} values avg"
        end 
    end

    #def to_s
    #puts "Decoder #{self.class} : records allowed(#{self.records_allowed.class}) => #{self.records_allowed}"
    #puts "Decoder #{self.class} : fields allowed(#{self.fields_allowed.class}) size#{self.fields_allowed.size} => #{self.fields_allowed}" 
    #end

    def self.test_decoder(klass_name)
        instance = create(klass_name)
        folder = File::dirname(__FILE__)
        file = klass_name.to_s.gsub("Decoder","").downcase
        file =  folder + "/" + file
        file = CDR::File.new(file,search:true)
        file.unzip! if file.zip?
        json = instance.decode(file,v: true)
        self.json_stats json
    end



end
