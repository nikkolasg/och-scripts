require_relative '../config/config'
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
        flow = App.flow(@flow_name)
        opts = { flow: @flow_name, filter: true, allowed: flow.records_allowed }
        json = CDR::decode(file_path, opts )
        Logger.<<(__FILE__,"INFO","Decoded #{file_path} from #{switch}") 
        json
    end

   
    #fields is an array of fields
    #arr is a matrix of array values
    def insert_decoded_record file_name,switch,record_name,fields, values

        table = App.flow(@flow_name).table_records(@dir)

        query = "INSERT INTO #{table} (switch,file_name,name, "
        query << fields.join(',') << ")"
        query << " VALUES "
        # collect all the values , making one entry for each record
        string_values_arr = values.map do |row|
            "('" + switch + "','" + file_name + "','" + record_name + "'," +
                row.map{|f| "'#{f}'"}.join(',') + ")"
        end
        query << string_values_arr.join(',') << ";"
        @db.query(query)

        Logger.<<(__FILE__,"INFO","Inserted  #{values.size} #{record_name} records from #{file_name} in db...") 
    end

    def insert_decoded_records file_name,switch,records
        records.each do |record|
            insert_decoded_record file_name,switch,record[:name],record[:fields],record[:values]
        end
    end

    # make the decoded files as processed in the cdr table 
    # so we wont decode them again next time .. !!!
    # input is a flat list of files
    def mark_processed_decoded_files files
        table = App.flow(@flow_name).table_cdr(@dir)
        sql = "UPDATE #{table} SET processed=1 WHERE file_name IN ( "
        sql << files.map{ |f| "'#{f}'"}.join(',') << ");"
        @db.query(sql)
        Logger.<<(__FILE__,"INFO","Mark as 'processed' #{files.size} files in db ...")
    end

    def backup_file switch, file
        require_relative '../get/fetchers'
        fetch = Fetchers::create(:LOCAL,{})
        oldp = App.directories.store(@dir) + "/" + switch
        newp = App.directories.backup(@dir) + "/" + switch
        fetch.download_files oldp,newp,[file]
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
            raise "Unkonwn flow file .. " unless App.flow(@flow)
        end

        def insert
            @db.connect do
                @files.each do |file|
                    file_path = @folder + "/" + file
                    records = decode_file @switch,file_path
                    next if records.nil?

                    insert_decoded_records file,@switch,records

                    mark_processed_decoded_files ([file])
                end
            end
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
            @flow_name = infos[:flow]
            @flow = App.flow(@flow_name)
            @folders = @flow.switches
            @cdr_table = @flow.table_cdr(@dir)
            @db = Datalayer::MysqlDatabase.default
        end

        def insert
            files,count = get_files_to_insert 
            Logger.<<(__FILE__,"INFO","Found #{count} files to decode & insert for #{@flow.name}:#{@dir}...");
            return unless count > 0
            @db.connect do 
                iterate_over files do |switch,file|
                    path = App.directories.store(@dir)+"/"+switch+"/"+file 

                    records = decode_file switch, path

                    if records.nil?
                        Logger.<<(__FILE__,"WARNING","Found null output for file #{file}")
                        next
                    end

                    insert_decoded_records file,switch,records

                    mark_processed_decoded_files ([file])
                    backup_file switch,file
                end
            end
            Logger.<<(__FILE__,"INFO","Decoded & Inserted #{count} files ...")
            Logger.<<(__FILE__,"INFO","Insert operation finished !")
        end

        ## sequential approach rather
        #than decode everything then insert everything
        # it sends the switch and file to the block
        # which will decode then insert
        def iterate_over files
            files.keys.each do |switch|
                files[switch].each do |file|
                    yield switch, file
                end
            end
        end



        ## retrieved files unprocessed
        # for the specified direction
        # return the files to get, and the total of files retrieved
        def get_files_to_insert 
            db = Datalayer::MysqlDatabase.default
            # SWITCHE1 => [file1,f2,f3...]
            files = Hash.new { |h,k| h[k] = []}
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
