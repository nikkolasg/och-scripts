module Getter
    require_relative '../ruby_util'
    require_relative '../util'
    require_relative './fetchers'
    require_relative '../datalayer'
    require_relative '../logger'
    require_relative '../config'
    require 'open3'
    require 'json'
    def self.create type, info = nil
        c = @@getters[type]
        if c
            c.new(info)
        else
            raise "Bad getter type #{type} ."
        end
    end

    @@getters = {}
    def self.register_getter type,name
        @@getters[type] = name
    end
    def get_saved_files
        db = Datalayer::MysqlDatabase.default
        files = {}
        table = EMMConfig["DB_TABLE_#{@flow}_CDR"]
        db.connect do 
            query = "SELECT file_name,switch FROM #{table} WHERE processed=0;"
            res = db.query query
            res.each_hash do |row|
                switch = row['switch']
                if !files[switch]
                    files[switch] = []
                end
                files[switch] << row['file_name']
            end
            Logger.<<(__FILE__,"INFO","Retrieved #{res.num_rows} entries from db...") 
        end
        files
    end
    # register theses files for the given switch
    def register_files_for_switch switch, files
        db = Datalayer::MysqlDatabase.default
        table = EMMConfig["DB_TABLE_#{@flow}_CDR"]
        db.connect do 
            sql = "INSERT INTO #{table} (file_name,switch)
                   VALUES "
            sql << files.map do |f|
                    "( '#{f}','#{switch}' )"
                end.join(',')
            sql << ";"
            db.query(sql)
        end
        Logger.<<(__FILE__,"INFO","Inserted #{files.size} files in #{table}")
    end

    # just a wrapper for registering files belonging
    # to different switches
    def register_host_files data
        data.each do |switch,files|
            register_files_for_switch switch,files
        end
    end

    class FilesGetter
        Getter.register_getter :files,self
        include Getter

        def initialize(infos)
            @dir = infos[:dir]
            @folder = infos[:folder] ? infos[:folder] : File.dirname(infos[:files].first)
            @files = infos[:files].map { |f| File.basename(f)} # just in case !
            @v = infos[:v]
            # try to guess the type
            @flow = Util.type(File.basename(@files.first))
            @switch = Util.switch(File.basename(@files.first))
            raise "Unknown type file... " unless @flow
        end

        def get
            db_files = get_saved_files
            filter_files db_files
            return unless @files.size > 0
            move_files
            register_files_for_switch @switch, @files
        end


        def filter_files db_files
            before = @files.size
            @files.select! do |file|

                !db_files.include? file
            end
            Logger.<<(__FILE__,"INFO","Filtering files. Before #{before} ==>  #{@files.size}..." )
        end
        
        def move_files
            fetch = Fetchers::FileFetcher.create(:local,{})
            new_dir = Util.data_path(EMMConfig["DATA_STORE_DIR"],@switch,{dir: @dir})
            fetch.download_files new_dir,@folder,@files
            Logger.<<(__FILE__,"INFO","Moved files from #{@folder} to #{new_dir}")
        end            
    end

    # responsible for handling the MSS get operations
    class MSSGetter
        Getter.register_getter :MSS,self
        
        include Getter

        def initialize(infos)
            @flow = :MSS
            # todo the direction is important
            @direction = infos[:dir]
            @hosts = RubyUtil::arrayize EMMConfig["MSS_HOSTS"]
            ## create the main structure used during the collection process
            @hosts = @hosts.inject({}) do |col,h|
                col[h] = {}
                Util::switches(h).each do |f|
                    col[h][f] = []
                end
                col
            end
            @fetchers = get_fetchers
            @v = infos[:v]
        end

        def get
            Logger.<<(__FILE__,"INFO", "Starting GET operations in #{self.class.name}.." )
            get_remote_files
            db_files = get_saved_files
            count = filter db_files 
            Logger.<<(__FILE__,"INFO","Filtering on remote files done ... ")
            if count > 0
                download_files
                move_files
                Logger.<<(__FILE__,"INFO","Files downloaded & moved into right folders ...")
            end
            @hosts.each do |host,switches|
                register_host_files switches
            end
            Logger.<<(__FILE__,"INFO","Files registered into the system ! ")
            Logger.<<(__FILE__,"INFO","GET Operation finished !")
        end

        private
        ## after downloaded files will be moved
        def move_files
            old_base_p = Util.data_path(EMMConfig["DATA_DOWN_DIR"])
            new_base_p = Util.data_path(EMMConfig["DATA_STORE_DIR"])
            @hosts.each do |h, sws|
                sws.each do |switch,files|
                    next if files.empty?
                    oldp = old_base_p + "/" + switch
                    newp = new_base_p + "/" + switch
                    cmd = "mv -t #{newp} #{files.map{|f| oldp+"/"+f}.join(" ")}"
                    if !system(cmd)
                        Logger.<<(__FILE__,"ERROR","with mv command : #{cmd}")
                        raise  "Error with mv command #{cmd}"
                    end
                end
            end     
        end

        def download_files
            remote_base_p = EMMConfig["MSS_BASE_DIR"] 
            local_base_p = Util.data_path(EMMConfig["DATA_DOWN_DIR"])
            @fetchers.each do |fetcher|
                fetcher.connect do 
                    @hosts[fetcher.host].each do |switch,files|
                        local = local_base_p + "/" + switch
                        remote = remote_base_p + "/" + switch
                        fetcher.download_files local,remote,files
                    end
                end
            end
        end

        ## filter the files to get
        # by the db and thoses already downloaded
        # (sometimes useful to testing multiple times )
        def filter saved_files
            count_to_dl = 0
            @hosts.keys.each do |h|
                break if saved_files.empty?
                switches = @hosts[h].keys & saved_files.keys
                next if switches.empty?

                switches.each do |sw|
                    str = "(DB)#{sw}: #{@hosts[h][sw].size} "
                    @hosts[h][sw] = @hosts[h][sw] - saved_files[sw]
                    count_to_dl = count_to_dl + @hosts[h][sw].size
                    str << "=> #{@hosts[h][sw].size}..."
                    Logger.<<(__FILE__,"INFO",str)
                end
            end
            ## FIlter by files contained in SERVER folder too !
            path = Util.data_path(EMMConfig["DATA_STORE_DIR"])
            @hosts.each do |h,sws|
                sws.each do |switch,files|
                    cmd = "ls #{path}/#{switch}"
                    out = `#{cmd}`
                    files_stored = out.split("\n")
                    str = "(LOCAL) #{switch}: #{files.size} "
                    @hosts[h][switch] = files - files_stored
                    count_to_dl = count_to_dl + @hosts[h][switch].size
                    str << " => #{@hosts[h][switch].size}..."
                    Logger.<<(__FILE__,"INFO",str)
                end
            end
            count_to_dl
        end

        def get_remote_files
            base_path = EMMConfig["MSS_BASE_DIR"] + "/"
            @fetchers.each do |fetcher|
                fetcher.connect do 
                    @hosts[fetcher.host].keys.each do |switch|
                        path = base_path + switch + "/"
                        @hosts[fetcher.host][switch] = @hosts[fetcher.host][switch] +  fetcher.list_files_from(path).to_a
                    end
                end
            end

        end
        def get_fetchers
            fetchers = []
            proto = EMMConfig["MSS_FETCH_PROTOCOL"].to_sym
            case proto
            when :sftp
                #careful if changing the layout of config file...
                credentials = {}
                credentials[:login] = EMMConfig["MSS_#{proto.upcase}_LOGIN"] 
                credentials[:pass] = EMMConfig["MSS_#{proto.upcase}_PASS"]
                @hosts.keys.each do |h|
                    credentials[:host] = h
                    fetchers << Fetchers::FileFetcher.create(proto,credentials)
                end
            end
            fetchers
        end

    end
end
