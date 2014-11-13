require_relative '../config'
require_relative '../logger'
require_relative '../util'
require_relative '../ruby_util'
require_relative '../database'
require_relative '../cdr'
module Inserter

    @@inserters = {}
    def self.create flow,infos
        infos[:flow] = flow
        c = @@inserters[flow]
        if c
            c.new(infos)
        else
            if App.flows.any? { |f| f.name == flow}
                c = @@inserters[:generic]
                c.new(infos)
            else
                raise "Bad inserter flow!"
            end
        end
    end

    def self.register_inserter flow,name
        @@inserters[flow] = name
    end

    # just make a nice printing to the log
    def log_file_summary file,records
        str = "#{file.name} has been decoded ( "
        arr = records.map do |record_type,value|
            "#{record_type} => #{value[:values].size}"
        end
        str << arr.join(',') << " )."
        return str
    end

    # INSERT IN THE DB the records with the fields / value
    #fields is an array of fields
    #arr is a matrix of array values
    def insert_decoded_record file_id,switch,record_name,fields, values

        table = @flow.table_records(@dir)

        query = "INSERT INTO #{table} (switch,file_id,name, "
        query << fields.join(',') << ")"
        query << " VALUES "
        # collect all the values , making one entry for each record
        string_values_arr = values.map do |row|
            "('" + switch + "','" + file_id + "','" + record_name + "'," +
                RubyUtil::sqlize(row,no_parenthesis: true) + ")"
        end
        query << string_values_arr.join(',') << ";"

        @db.query(query)

    end

    # simple proxy to insert every records at same time
    # will dispatch by records type
    def insert_decoded_records file_id,switch,records
        records.each do |record_name,record|
            insert_decoded_record file_id,switch,record_name,record[:fields],record[:values]
        end
    end

    # make the decoded files as processed in the cdr table 
    # so we wont decode them again next time .. !!!
    # input is a flat list of files (CDR::File)
    def mark_processed_decoded_files files_ids
        table = @flow.table_cdr(@dir)
        sql = "UPDATE #{table} SET processed=1 WHERE file_id IN "
        sql << RubyUtil::sqlize(files_ids,no_quote:true)
        @db.query(sql)
        #Logger.<<(__FILE__,"INFO","Mark as 'processed' #{files.size} files in db ...")
    end

    ## compress a file and move it to the backup folder
    def backup_file switch,file
        @manager ||= App::LocalFileManager.new
        newp = App.directories.backup(@dir) + "/" + switch
        file.zip!
        @manager.move_files [file],newp
    end

    # find the "source" object from the switch name and the direction
    # return nil ortherwise
    def find_source switch,dir
        @flow.sources(dir).each { |s| return s if s.switches.include? switch }
    end

    class FileInserter
        Inserter.register_inserter :files, self
        include Inserter
        def initialize(infos)
            @dir = infos[:dir]
            @folder = infos[:folder] ? infos[:folder] : File.dirname(infos[:files].first)
            @files = infos[:files].map { |f| CDR::File(File.basename(f),@folder) }
            @v = infos[:v]
            @flow_name = Util.flow(@files.first.name)
            @switch = Util.switch(@files.first.name)
            @db = Database::Mysql.default
            raise "File Inserter : Flow unknown" unless @flow = App.flow(@flow)
            find_decoder
        end

        def insert
            @db.connect do
                @files.each do |file|
                    records = @decoder.decode file
                    next if records.nil?

                    insert_decoded_records file,@switch,records

                    mark_processed_decoded_files ([file])
                end
            end
        end

        # try to find the right decoder either from console
        # or from a Source definition
        def find_decoder opts =  {}
            if opts[:decoder]
                @decoder = Decoder::create(opts[:decoder],opts[:records],opts[:fields],opts)
            else
                @decoder = find_source(@switch,@dir).decoder
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
            @opts_dir = infos[:dir].downcase.to_sym
            @flow_name = infos[:flow]
            @flow = App.flow(@flow_name)
            @folders = @flow.switches
            @db = Database::Mysql.default
        end

        def insert
            @db.connect do 
                Util::starts_for(@opts_dir) do |dir|
                    @dir = dir
                    @cdr_table = @flow.table_cdr(@dir)
                    insert_ 
                end
            end
        end
        # insertion method for a specific direction
        def insert_
            files,count = get_files_to_insert 
            Logger.<<(__FILE__,"INFO","Found #{count} files to decode & insert for #{@flow.name}:#{@dir}...");
            return unless count > 0
            base_path =  App.directories.store(@dir)
            ids_processed = []
            file_counter = 1
            ## iterate over switch ==> file_id, file_name
            iterate_over files do |switch,id,name|
                file_path = base_path + "/" + switch + "/" + name
                file = CDR::File.new(file_path,search: true)
                records = @decoder.decode file
                if records.nil?
                    Logger.<<(__FILE__,"WARNING","Found null output for file #{file}")
                    next
                end
                insert_decoded_records id,switch,records
                #ids_processed << id
                mark_processed_decoded_files([id])
                backup_file switch,file
                str = log_file_summary file,records
                Logger.<<(__FILE__,"INFO","(#{file_counter}/#{count}) #{str}")
                file_counter += 1
            end
            # so only one lookup for table cdr
            #mark_processed_decoded_files (ids_processed)
            Logger.<<(__FILE__,"INFO","Decoded & Inserted #{count} files ...")
            Logger.<<(__FILE__,"INFO","Insert operation finished !")
        end

        ## sequential approach rather
        #than decode everything then insert everything
        # it sends the switch and file to the block
        # which will decode then insert
        def iterate_over files
            files.keys.each do |switch|
                @decoder = find_source(switch,@dir).decoder
                files[switch].each do |id,name|
                    yield switch,id,name
                end
            end
        end


        ## retrieved files unprocessed
        # for the specified direction
        # return the files to get, and the total of files retrieved
        def get_files_to_insert 
            db = Database::Mysql.default
            # SWITCHE1 => [file1,f2,f3...]
            files = Hash.new { |h,k| h[k] = []}
            count = 0
            db.connect do 
                query = "SELECT file_id, file_name,switch FROM #{@cdr_table} WHERE processed=0;"
                res = db.query(query)
                res.each_hash do |row|
                    files[row['switch']] << [row['file_id'],row['file_name']]
                    count = count + 1
                end
            end
            return files,count
        end

    end
end
