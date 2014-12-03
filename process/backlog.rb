module Stats


    ## That class with take a source and will get/decode & process 
    #all its files until a certain date
    # it does way more work than genericstats, but
    # it does not store the RECORDS in the database nor the files associated
    # only records in a *_BACKLOG table what files it has already processed
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

    ## MAIN Method
    #    def compute
    #@db.connect do 
    #@monitors.each do |mon|
    #@current = mon
    #@current.schema.set_db @db
    #@current.sources.each do |source|
    #@source = source
    #@manager = source.file_manager
    #@manager.config(@opts)
    #files = get_files2dl
    #count = 1
    #@num_rows = files.values.flatten.size
    ### MAIN PART : decode / analyze / store
    #compute_files(files) do |folder,file|
    #Logger.<<(__FILE__,"INFO","(#{count}/#{@num_rows}) Analyzing file #{file.name}")
    ### Processing part
    #file.unzip! if file.zip?
    #json = @source.decoder.decode file
    #upgrade_folder(folder,json)
    #analyze_json json

    ### Insertion part
    #@current.schema.insert_stats @source
    #@current.reset_stats
    #@current.schema.backlog_processed_files @source,[file]
    #count += 1
    #progression(count)
    #end
    #Logger.<<(__FILE__,"INFO","Having processed #{@records_count} records from source #{@source}")
    #@records_count = 0
    #end

    #end
    #end
    #end

    def compute
        @sources = @monitors.map { |m| m.sources }.flatten.uniq
        Logger.<<(__FILE__,"INFO","Will process backlog for monitors #{@monitors.map{ |m| m.name}.join(',')}")
        path = Conf::directories.store
        @db.connect do 
            get_saved_files ## retrieved ALL files already processed in DB
            @sources.each do |source|
                @manager = source.file_manager
                @manager.config(@opts)
                @source = source
                files = get_files
                count = 1
                @num_rows = files.values.flatten.size
                ## Actually download each file and yield it
                compute_files(files) do |folder,file|
                    str = "(#{count}/#{@num_rows}) Analyzing file #{file.name} ("
                    @monitors.each do |mon|
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
                        json ||= @source.decoder.decode file
                        str += "#{mon.name}:O "
                        analyze_json json
                        @current.schema.insert_stats @source
                        @current.reset_stats
                        @current.schema.backlog_processed_files @source,[file]
                    end
                    Logger.<<(__FILE__,"INFO",str+")")
                    count += 1
                    progression(count)
                    json = nil
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
            fields = @current.filter.filter_fields(fields) if @current.filter
            values.each do |record|
                @records_count += 1
                next if (@current.filter && !@current.filter.filter_record(record))
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
        ## MONITOR => SOURCES => FILES (hash Filename => true) (for speed)
        @files2rm = Hash.new { |h,k| h[k] = {} }
        @monitors.each do |mon|
            mon.sources.each do |source|
                f2rm = mon.schema.backlog_saved_files source
                @files2rm[mon.name][source.name] = f2rm.inject(Set.new) { |col,f| col << f; col}
            end
        end
    end

    ## Will filter files for a source & a monitor (@source / @current)
    def allowed? file
        f2rm = @files2rm[@current.name][@source.name]
        v = f2rm.include?(file) ? false : true ## if file not preset (i.e.nil)
        return v
        # return true, if preset, ret false
    end

    # def filter_files files
    #f2rm = @current.schema.backlog_saved_files @source
    #files.each do |folder,files_list|
    #ocount = files_list.size
    #files_list = files_list - f2rm
    #files[folder] = files_list
    #Logger.<<(__FILE__,"INFO","Filtering on #{folder} : #{ocount} => #{files_list.size}")
    #end
    #return files
    #end
end
