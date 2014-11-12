module Stats


    ## That class with take a source and will get/decode & process 
    #all its files until a certain date
    # it does way more work than genericstats, but
    # it does not store the RECORDS in the database nor the files associated
    # only records in a *_BACKLOG table what files it has already processed
    class BacklogStats
        include Stats

        # subfactory method to differentiate between backlog operations
        def self.create(type,opts)
            case type
            when :monitor
                return MonitorBacklogStats.new(opts[:monitor],opts)
            when :flow
                return FlowBacklogStats.new(opts[:flow],opts)
            end
        end 

        def initialize
            @monitors_filters_done = Hash.new {|h,k| h[k] = {} }
            ## definition see GenericStats
            @monitors_stats = Hash.new do |hash,key|
                dir = { input: 0 , output: 0 }
                sub_hash = Hash.new { |h,k| h[k] = dir.clone }
                # monitor => time => type => dir => count
                hash[key] = Hash.new { |h,k| h[k] = sub_hash.clone }
            end
            ## sets the right date for the file_manager
            @sources.each { |s| s.file_manager.config(@opts) }
            @files = {}
        end 

        ## MAIN Method
        def compute
            @sources.each do |source|
                @source = source
                @dir = source.direction
                @manager = source.file_manager
                str = "Will process files from #{source.name} (#{@source.host}"
                Logger.<<(__FILE__,"INFO",str)
                str = "Processing  from #{@manager.min_date_filter} to #{@manager.max_date_filter} "
                Logger.<<(__FILE__,"INFO",str)
                compute_source # make the comp for this source
            end
        end

        # run for this source only
        def compute_source
            @manager.start do 
                list_files # get remote files
                filter_files # only keep the new ones
                if @files.empty?
                    Logger.<<(__FILE__,"INFO","No file to be analyzed. Either no file " +
                              "or all files are already in the system!")
                    return
                end
                download_files # download them
            end
            make_stats
            @db = Database::Mysql.default
            @db.connect do
                @monitors.each do |m|
                    @current = m
                    format_stats 
                    mark_as_processed
                end
            end
            delete_files
            @files = {}
        end

        def delete_files
            man = App::LocalFileManager.new
            man.delete_files *@files.keys
        end

        def mark_as_processed
            table = @current.table_records_backlog
            sql = "INSERT INTO #{table} (file_name) VALUES " +
                @files.keys.map { |f| "('#{f.name}')" }.join(',') +
                ";"
            Logger.<<(__FILE__,"DEBUG",sql)
            @db.query(sql)
        end

        # get the files from the source
        # organized by FILE => SWITCH hash
        def list_files
            @source.switches.each do |sw|
                f = @manager.find_files(sw).inject({}) { |col,f| col[f] = sw; col }
                @files.merge! f
                Logger.<<(__FILE__,"INFO","Found #{f.keys.size} files from #{sw} on #{@source.name} .")
            end
        end
        ## filter out the files that are contained in the
        # CDR table for the flow
        # It will also grab a list of file_name that monitors
        # have already done in backlog so they dont have to compute
        # two times for same file
        def filter_files
            old_count = @files.size
            files_name = @files.keys.map { |f| f.name } 
            table = @flow.table_cdr(@dir)
            sql = "SELECT * FROM #{table} WHERE file_name IN "
            sql += RubyUtil::sqlize(files_name) 
            sql += ";"
            db = Database::Mysql.default
            db.connect do 
                res =  db.query(sql)
                return unless res
                res.each_hash do |row|
                    @files.delete_if do |f,sw|
                        f.name == row['file_name']
                    end
                end

                @processed_files = {}
                @monitors.each do |m|
                    sql = "SELECT m.file_name FROM #{m.table_records_backlog} as m " +
                        " WHERE m.file_name IN "
                    sql += RubyUtil::sqlize(files_name)
                    sql += ";"
                    res = db.query(sql)    
                    res.each_hash do |row|
                        @files.delete_if do |f,sw|
                            f.name == row['file_name']
                        end
                    end
                end
            end
            # LOCAL filtering
            @files_dl = @files.clone
            man = App::LocalFileManager.new
            @source.switches.each do |sw|
                entries = man.ls (App.directories.store + "/" + sw)
                str = "Filtering entries for #{sw} ..."
                count = @files_dl.size
                @files_dl.keys.delete_if do |f|
                    res = entries.include?(f.cname)
                    res
                end
                #Logger.<<(__FILE__,"DEBUG",str + " #{count - @files_dl.size} deleted")
            end

            Logger.<<(__FILE__,"INFO","Filtering, #{old_count} => #{@files_dl.keys.size} left.")
        end


        ## Then download the files on the machine
        def download_files
            sw = @files_dl.inject(Hash.new { |h,k| h[k] = [] } ) do |col,(f,sw)|
                col[sw] << f
                col
            end
            sw.each do |switch,files|
                npath = App.directories.store + "/" + switch
                @manager.download_files files,npath
            end
        end
        # And now, we compute the stats
        # decode => analyse => store
        def make_stats
            decoder = @source.decoder
            counter = 0
            @num_rows = @files.size
            @files.each do |file,switch|
                file.unzip! if file.zip?
                json = decoder.decode file
                json.each do |name,hash|
                    fields = {}
                    ## transform into hash TYPE => index in arr
                    hash[:fields].each_with_index do |field,index|
                        fields[field.to_sym] = index
                    end
                    hash[:values].each do |row|

                        @monitors.each do |m|
                            @current = m
                            ## reject if this record does not come from 
                            #the right place for this monitor
                            next unless @current.switches.include? switch
                            next if @current.filters_records unless @current.filter_records.include? name
                            next unless allowed? fields,row
                            time = get_time_from_row fields,row
                            formatted_time = get_formatted_time time
                            @monitors_stats[@current.name][formatted_time][name][@dir] += 1
                        end
                    end
                end
                progression counter
                counter += 1
            end
        end

        def get_time_from_row fields,row
            field_time = @flow.time_field_records.downcase.to_sym
            row[fields[field_time]]
        end

        def allowed? fields,row
            # check if all filters return true !
            @current.filters.each do |field,block|
                # check if we have results cached
                if @monitors_filters_done[@current.name].key? field 
                    # this filter let pass , so we check next on
                    if @monitors_filters_done[@current.name][field] == true
                        next
                    else # this filter does not pass so directly say no !
                        return false
                    end
                end

                # caching results
                out = block.call(record[field])
                @monitors_filters_done[@current.name][field] = out

                if out == true
                    next
                else
                    return false
                end
            end
            return true


        end

    end
    class FlowBacklogStats < BacklogStats
        def initialize flow,opts
            @flow = flow
            @sources = @flow.sources 
            @monitors = @flow.monitors
            @opts = opts
            super()
        end
    end

    class MonitorBacklogStats < BacklogStats
        def initialize monitor,opts
            @monitors = RubyUtil::arrayize monitor
            @sources = monitor.sources
            @flow = monitor.flow
            @opts = opts
            super()
        end

    end

end
