module Stats


    ## That class with take a source and will get/decode & process 
    #all its files until a certain date
    # It does not go by the usual process ==> 
    # get, decoder & process are done in a one-way fashion, and no storing is used 
    # except the file_name retrived (to not process duplicate)
    # the filename are stored into a special table of the monitor
    class BacklogStats
        include Stats
        require 'set'
        # subfactory method to differentiate between backlog operations
        def self.create(opts)
            opts[:monitor] = opts[:flow].monitors if opts[:flow]
            BacklogStats.new(opts)
        end
    end 

    def initialize opts
        @monitors = RubyUtil::arrayize(opts[:monitor])
        ## sets the right date for the file_manager
        @files = {}
        ## current monitor
        @current = nil
        @opts = opts
        @db = Database::Mysql.default
        str = "Backlog processing  from #{opts[:min_date]} to #{opts[:max_date]} "
        Logger.<<(__FILE__,"INFO",str)
        @records_count = 0 ## to keep track of how many records have we analyzed

    end 

    ##
    #  MAIN method =)
    # download, decode , process each file for each source corresponding to the crierions
    # ##############
    def compute
        @sources = @monitors.map { |m| m.sources }.flatten.uniq
        Logger.<<(__FILE__,"INFO","Will process backlog for monitors #{@monitors.map{ |m| m.name}.join(',')}")
        path = Conf::directories.store
        @db.connect do 
            SignalHandler.check { Logger.<<(__FILE__,"WARNING","SIGNINT catched .Abort.");@db.close}

            get_saved_files ## retrieved ALL files already processed in DB
            @sources.each do |source|
                ## Must move get saved_file here so we check on the file GET module has
                SignalHandler.check { Logger.<<(__FILE__,"WARNING","SIGNINT catched .Abort.");@db.close}

                @manager = source.file_manager
                @source = source
                @source.set_options(@opts)
                files = get_files
                count = 1
                @num_rows = files.values.flatten.size
                ## Actually analyze each file and yield it. can be downloaded if wnated
                compute_files(files) do |folder,file|
                    str = "(#{count}/#{@num_rows}) Analyzing file #{file.name} ("
                    @monitors.each do |mon|
                        SignalHandler.check { Logger.>>(__FILE__,"WARNING","SIGINT Catched. Abort");@db.close } 
                        @current = mon
                        @current.schema.set_db @db
                        next unless mon.sources.include? @source
                        unless allowed?(file)
                            str += "#{mon.name}:X "
                            next
                        end
                        ## only download the file if needed
                        @manager.download_files [file],path unless file.downloaded?
                        file.unzip! if file.zip?
                        ## decode one time only
                        json ||= @source.decoder.decode file
                        str += "#{mon.name}:O "
                        analyze_json json
                        @current.schema.insert_stats @source
                        @current.reset_stats
                        @current.schema.backlog_processed_files @source,[file]
                        SignalHandler.check { Logger.<<(__FILE__,"WARNING","SIGNINT catched .Abort.");@db.close}
                    end
                    Logger.<<(__FILE__,"INFO",str+")")
                    count += 1
                    progression(count)
                    json = nil
                    SignalHandler.check { Logger.<<(__FILE__,"WARNING","SIGNINT catched .Abort.");@db.close}

                end
                progression(0,reset:true) 
                Logger.<<(__FILE__,"INFO","Having processed #{@records_count} records from source #{@source}")
                @records_count = 0
            end
        end
    end


    def analyze_json json
        json.each do |name,hash|
            fields = hash[:fields]
            values = hash[:values]
            values.each do |record|
                @records_count += 1
                analyze_record(fields,record) 
            end
        end
    end

    ## In the case of normal processing, the "folder" field is automatically
    #retrieved from the db. Here it is NOT in the JSON output of the decoder
    #This methods adds the folder field to the fields & values of this hash
    def upgrade_folder folder,json
        json.each do |name,hash|
            hash[:fields][:folder] = hash[:values].first.size 
            hash[:values].map! { |row| row << folder }
        end

    end

    def analyze_record fields,record
        record_time = get_time_from_record fields,record 
        formatted_time = get_formatted_time record_time
        @current.stats.analyze formatted_time,fields,record
    end

    # run for this source only
    # Will retrieve files found on the source that corresponds to the 
    # search criterias (date =)
    def get_files
        files = []
        @manager.start do 
            files = list_files # get remote files
            if files.empty?
                Logger.<<(__FILE__,"INFO","No file to be analyzed. Either no file " +
                          "or all files are already in the system!")
                return
            end
        end
        return files
    end

    ## Main method that will download , decode , and analyze
    #  ONE file at A TIME
    def compute_files files
        @manager.start do 
            files.each do |folder,files|
                next if files.empty?
                files.each do |file|
                    yield folder,file
                    Conf::LocalFileManager.new.delete_files(file) if file.downloaded?
                end
            end
        end 
    end

    private

    def get_time_from_record fields,record
        time_field = @current.flow.time_field_records
        record[fields[time_field]]
    end

    # organized by FILE => SWITCH hash
    def list_files
        files = {}
        @source.folders.each do |sw|
            h = Hash.new { |h,k| h[k] = [] }
            f = @manager.find_files(sw).inject(h) { |col,f| col[sw] << f; col }
            files.merge! f
            Logger.<<(__FILE__,"INFO","Found #{f[sw].size} files from #{sw} on #{@source.name} .")
        end
        files
    end

    ## Actually retrieve all saved files for all monitors for all sources
    ## store it
    def get_saved_files
        ## MONITOR => SOURCES => FILES (Set) (for speed)
        @files2rm = Hash.new { |h,k| h[k] = {} }
        @monitors.each do |mon|
            mon.sources.each do |source|
                f2rm = mon.schema.backlog_saved_files source
                @files2rm[mon.name][source.name] = f2rm.inject(Set.new) { |col,f| col << f; col}
            end
        end
    end
    
    ## acutally retrieves the files in backlog + from the source schema ^FILE table
    ## i.e. the "normal" table
    def get_source_saved_files
        @files2rm = Hash.new { |h,k| h[k] = {} }
    end

    ## Will filter files for a source & a monitor (@source / @current)
    def allowed? file
        f2rm = @files2rm[@current.name][@source.name]
        v = f2rm.include?(file) ? false : true ## if file not preset (i.e.nil)
        return v
    end

end
