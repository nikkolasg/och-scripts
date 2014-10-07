require_relative '../config'
require_relative '../logger'
require_relative '../util'
require_relative '../ruby_util'
require_relative '../datalayer'
require_relative '../cdr'
module Inserter

    @@inserters = {}
    def self.create flow,infos
        infos[:flow] = flow
        c = @@inserters[flow]
        if c
            c.new(infos)
        else
            raise "Bad inserter flow!"
        end
    end

    def self.register_inserter flow,name
        @@inserters[flow] = name
    end

    def decode_file switch, file_path

        json = CDR::decode(file_path, decoder_options )
        Logger.<<(__FILE__,"INFO","Decoded #{file_path}, from #{switch}") if @v
        json
    end

    def decoder_options
        if @flow == :MSS
            allowed = EMMConfig["MSS_RECORDS"].join(',')
            { flow: @flow, filter: true, allowed: allowed}
        end
    end
    # insert the records information in the database
    # decoded is the json from CDR::decode
    # switch is the name of the switch it comes from
    # file is the name of the file it comes from
    # dir is the direction of the decoded flow (input / output)
    def insert_decoded_files decoded
        config_name = "DB_TABLE_#{@flow}_RECORDS"
        config_name << "_OUT" unless @dir == :input

        table = EMMConfig[config_name]
            decoded.keys.each do |switch|
                decoded[switch].each do |file,arr|
                    log={}
                    arr.each do |hash|
                        name = hash[:name]
                        fields = hash[:fields]
                
                        query = "INSERT INTO #{table} (switch,file_name,name, "
                        query << fields.join(',') << ")"
                        query << " VALUES "
                        # collect all the values , making one entry for each record
                        values = hash[:values].map do |row|
                            "('" + switch + "','" + file + "','" + name + "'," +
                                row.map{|f| "'#{f}'"}.join(',') + ")"
                        end
                        query << values.join(',') << ";"
                        @db.query(query)
                        log[name] = hash[:values].size
                    end
                    Logger.<<(__FILE__,"INFO","Inserted  #{log.inject("") { |c,(name,size)| c = c + size.to_s + " " + name + ", " }}} from #{file} in db...") if @v
                end
        end
    end

    class FileInserter
        Inserter.register_inserter :files, self
        include Inserter
        def initialize(infos)
            @dir = infos[:dir]
            @folder = infos[:folder] ? infos[:folder] : File.dirname(infos[:files].first)
            @files = infos[:files].map { |f| File.basename(f) }
            @v = infos[:v]
            @flow = Util.flow(File.basename(@files.first))
            @switch = Util.switch(File.basename(@files.first))
            @db = Datalayer::MysqlDatabase.default
            raise "Unkonwn flow file .. " unless @flow
        end

        def insert
            decoded = {}
            decoded[@switch] = @files.inject({}) do |col,file|
                file_path = @folder + "/" + file
                col[file] = decode_file @switch,file_path
                col
            end
            Logger.<<(__FILE__,"INFO","Decoded #{@files.size} files ...")
            @db.connect do 
                insert_decoded_files decoded
            end
            Logger.<<(__FILE__,"INFO","Inserted all files ! Operation finished !")

        end

    end

    ## generic class that handles the insertion of flow
    # for both direction
    # TO USE FOR FLOW ! because it fetch the list of files to take 
    # from the db. Uss FilesInserter to test with files
    class GenericFlowInserter

        Inserter.register_inserter :MSS,self
        # register others flow !
        include Inserter
        def initialize(infos)
            @v = infos[:v]
            @dir = infos[:dir].downcase.to_sym
            @flow = infos[:flow].upcase.to_sym
            @folders = Util.folders @flow
            name_table_config = @dir == :input ? "DB_TABLE_#{@flow}_CDR" : "DB_TABLE_#{@flow}_CDR_OUT"
            @cdr_table = EMMConfig[name_table_config]
            @db = Datalayer::MysqlDatabase.default
        end

        def insert
            if @dir == :input
                insert_input
            elsif @dir == :output
                insert_output
            end
        end

        def insert_input
            files,count = get_files_to_insert 
            Logger.<<(__FILE__,"INFO","Found #{count} files to decode & insert for #{@flow}:input...");
            return unless count > 0
            @db.connect do 
                decode_files files do |switch,file|
                    decoded = { switch => {} }
                    path = Util.data_path(EMMConfig["DATA_STORE_DIR"],switch,file,{dir: @dir})
                    decoded[switch][file] = decode_file switch, path
                    insert_decoded_files decoded
                end
            end
            Logger.<<(__FILE__,"INFO","Decoded & Inserted #{count} files ...")
        end

        def insert_output

        end


        def decode_files files
            decoded = {}
            files.keys.each do |switch|
                decoded[switch] = {}
                files[switch].each do |file|
                    yield switch, file
                end
            end
            decoded
        end



        ## retrieved files unprocessed
        # for the specified direction
        # return the files to get, and the total of files retrieved
        def get_files_to_insert 
            db = Datalayer::MysqlDatabase.default
            # SWITCHE1 => [file1,f2,f3...]
            files = Hash[RubyUtil::arrayize(EMMConfig["#{@flow}_SWITCHES"]).map{|s| [s,[]]}]
            count = 0
            db.connect do 
                query = "SELECT file_name,switch FROM #{@cdr_table} WHERE processed=0;"
                res = db.query(query)
                res.each_hash do |row|
                    files[row['switch']] << row['file_name']
                    count = count + 1
                end
            end
            return files,count
        end

    end
end
