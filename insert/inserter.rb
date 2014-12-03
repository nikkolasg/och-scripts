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
        str = "Decoded : #{file.name} ("
        arr = records.map do |record_type,value|
            "#{record_type} => #{value[:values].size}"
        end
        str << arr.join(',') << ")"
        return str
    end

    ## compress a file and move it to the backup folder
    def backup_file folder,file
        @manager ||= Conf::LocalFileManager.new
        newp = File.join(Conf::directories.backup,@curr_source.name.to_s,folder)
        file.zip!
        @manager.move_files [file],newp
    end


    ## generic class that handles the insertion of flow
    # for both direction
    # TO USE FOR FLOW ! because it fetch the list of files to take 
    # from the db. Uss FilesInserter to test with files
    class GenericFlowInserter
        require_relative '../debugger'
        Conf::flows.each do |flow|
            Inserter.register_inserter flow.name,self
        end
        # register others flow !
        include Inserter
        def initialize(infos)
            @v = infos[:v]
            @flow_name = infos[:flow]
            @flow = Conf::flow(@flow_name)
            @sources = infos[:source] || @flow.sources
            @db = Database::Mysql.default
            @curr_source = nil
            @opts = infos
        end

        def insert
            @db.connect do 
                @sources.each do |source|
                    @curr_source = source
                    @curr_schema = source.schema
                    @curr_schema.set_db @db
                    @curr_source.decoder.opts.merge! @opts
                    insert_ 
                end
            end
        end
        # insertion method for a specific source
        def insert_
            # new files to insert
            nfiles = @curr_schema.select_new_files
            count = nfiles.size
            Logger.<<(__FILE__,"INFO","Found #{nfiles.size} files to decode & insert for #{@curr_source.name}...");
            return unless nfiles.size > 0

            ## Will decode them and insert their records, one by one (file)
            base_path =  Conf::directories.store + "/" + @curr_source.name.to_s
            ids_processed = []
            file_counter = 0
            iterate_over nfiles do |file|
                file_path = File.join(base_path,file[:folder],file[:file_name])
                file_ = CDR::File.new(file_path,search: true)
                records = @curr_source.decoder.decode file_
                if records.nil?
                    Logger.<<(__FILE__,"WARNING","Found null output for file #{file}")
                    next
                end
                @curr_schema.insert_records file[:file_id], records
                @curr_schema.processed_files RubyUtil::arrayize(file[:file_id])
                backup_file file[:folder],file_
                str = log_file_summary file_,records
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
            files.each do |file|
                yield file
            end
        end
    end
end
