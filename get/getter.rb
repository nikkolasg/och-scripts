module Getter
    require_relative '../ruby_util'
    require_relative '../util'
    require_relative './fetchers'
    require_relative '../datalayer'
    require_relative '../logger'
    require_relative '../config/config'
    require 'open3'
    require 'json'
    def self.create type, info = nil
        c = @@getters[type]
        info[:flow] = type
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

    ## 
    # Will return the files that are already contained in the database
    # from the given list of files
    def filter_db_files remote_files
        db = Datalayer::MysqlDatabase.default
        files = {}
        table = App.flow(@flow_name).table_cdr(@direction)
        db.connect do 
            query = "SELECT file_name,switch FROM #{table} WHERE file_name IN #{RubyUtil::sqlize remote_files};"
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
        table = App.flow(@flow_name).table_cdr(@direction)
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

    class FilesGetter
        Getter.register_getter :files,self
        include Getter

        def initialize(infos)
            @dir = infos[:dir]
            @folder = infos[:folder] ? infos[:folder] : File.dirname(infos[:files].first)
            @files = infos[:files].map { |f| File.basename(f)} # just in case !
            @v = infos[:v]
            # try to guess the type
            @flow_name = Util.type(File.basename(@files.first))
            @switch = Util.switch(File.basename(@files.first))
            raise "Unknown type file... " unless @flow
        end

        def get
            filter_files 
            return unless @files.size > 0
            move_files
            register_files_for_switch @switch, @files
        end


        def filter_files
            # return the files in @files also contained in the db
            db_files = filter_db_files @files
            before = @files.size
            @files.select! do |file|
                !db_files.include? file
            end
            Logger.<<(__FILE__,"INFO","Filtering files. Before #{before} ==>  #{@files.size}..." )
        end

        def move_files
            fetch = Fetchers::create(:LOCAL,{})
            new_dir = App.directories.store(@dir) + "/" + @switch
            fetch.download_files new_dir,@folder,@files
            Logger.<<(__FILE__,"INFO","Moved files from #{@folder} to #{new_dir}")
        end            
    end

    # responsible for handling the flows get operations
    class GenericFlowGetter
        Getter.register_getter :MSS,self
        include Getter

        def initialize(infos)
            @flow = App.flow(infos[:flow])
            @flow_name = infos[:flow]
            # todo the direction is important
            @direction = infos[:dir]
            @sources = @flow.sources(@direction)
            ## structure that contains the files
            # organized by switches ( hash)
            @files = @sources.inject({}) do |col,source|
                source.switches.each { |s| col[s] = [] }
                col
            end

            ## structure that relates the source to
            # a fetcher for this source
            @sources = @sources.map do |source|
                [source,Fetchers::create(source.protocol,
                                         source.host,
                                         source.login,
                                         source.password) ]
            end
            @v = infos[:v]
        end

        def get
            Logger.<<(__FILE__,"INFO", "Starting GET operations in #{self.class.name}.." )
            get_remote_files
            filter
            list,count = list_to_download # filter out files already downloaded:
            Logger.<<(__FILE__,"INFO","Filtering on remote files done ... will download #{count} files. ")
            if count > 0
                download_files list
                move_files list
                Logger.<<(__FILE__,"INFO","Files downloaded & moved into right folders ...")
            end
            @files.each do |switch,list_files|
                register_files_for_switch switch,list_files
            end
            Logger.<<(__FILE__,"INFO","Files registered into the system ! ")
            Logger.<<(__FILE__,"INFO","GET Operation finished !")
        end

        private
        ## after downloaded files will be moved
        def move_files files_to_dl
            old_base_p = App.directories.tmp(@direction)
            new_base_p = App.directories.store(@direction)
            fetcher = Fetchers::create(:LOCAL,{})
            files_to_dl.each do |switch,list_files|
                next if list_files.empty?
                oldp = old_base_p + "/" + switch
                newp = new_base_p + "/" + switch
                fetcher.download_files newp,oldp,list_files
            end
        end     

        ## actually take the data to the app
        def download_files files_to_dl
            local_base_p = App.directories.tmp(@direction)
            @sources.each do |source,fetcher|
                remote_base_p = source.base_dir 
                fetcher.connect do 
                    source.switches.each do |switch|
                        files = files_to_dl[switch]
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
        # return the files to download
        def filter 
            # get files contained in the list and also in db
            # ==> files to eliminate
            f = @files.values.flatten(1)
            saved_files = filter_db_files f
            count_to_dl = 0
            return count_to_dl if saved_files.empty?
            #intersection of switches
            # limit the comparison between db and files
            switches = @files.keys & saved_files.keys
            return count_to_dl if switches.empty?

            # THESE files wont have to be download
            # so we remove them from the global list
            switches.each do |sw|
                str = "(DB)#{sw}: #{@files[sw].size} "
                @files[sw] = @files[sw] - saved_files[sw]
                count_to_dl = count_to_dl + @files[sw].size
                str << "=> #{@files[sw].size}..."
                Logger.<<(__FILE__,"INFO",str)
            end

            return count_to_dl
        end
        def list_to_download
            ## FIlter by files contained in SERVER folder too !
            #file NOT contained in the server, will have to be downloaded
            # SO we create a list of files to download, which is by definition
            # also contained in the global list (since .clone)
            count_to_dl = 0
            path = App.directories.store(@direction)
            local_fetch = Fetchers::create(:LOCAL,{})
            files_to_dl = @files.clone
            files_to_dl.each do |switch,list_files|
                _path = "#{path}/#{switch}"
                # get files already downloaded for theses switches
                files_stored = local_fetch.list_files_from _path   
                # simple out put
                str = "(LOCAL) #{switch}: #{list_files.size} "

                files_to_dl[switch] = list_files - files_stored
                count_to_dl = count_to_dl + files_to_dl[switch].size

                str << " => #{files_to_dl[switch].size}..."
                Logger.<<(__FILE__,"INFO",str)
            end
            return files_to_dl,count_to_dl
        end

        def get_remote_files
            @sources.each do |source,fetcher|
                fetcher.connect do 

                    source.switches.each do |switch|
                        path = source.base_dir + "/" + switch + "/"
                        @files[switch] = @files[switch] +  fetcher.list_files_from(path).to_a
                    end
                end
            end

        end

    end

    # represent a "source"
    # middle class that handles the creation of fetcher
    # the listing of files 
    # and the downloading !
    # very usefule for a clean code because of the complexity of
    # the different sources => cdr into multiple subfolders etc etc
    class Source

        def initialize(configSource)
            @conf = configSource
            @data = Hash[ @conf.switches.map{|s| [s,[]] }] unless @conf.sub_folders
            @data = Hash[ @conf.switches.map{|s| [s,{}] }] if @conf.sub_folders
            @fetcher = Fetcher::create(@conf.protocol,@conf.host,@conf.login,@conf.password)
        end

        # return an hash of files present on the source
        # key : switch
        # values : files
        def list_files
            @fetcher.connect do 
                @conf.switches.each do |switch|
                    if !@conf.sub_folders ## no subfolders
                    else ## subfolder ...

                        folders = subfolders_listing
                    end
                end
            end
        end

        private

        # simply list all files directly from the different switches
        def simple_listing switch

            path = @conf.base_dir + "/" + switch
            @data[switch] =@fetcher.list_files_from path
        end 

        # list all files contained in sub folders.
        # Will take the N first folders specified in 
        # the conf as "sub_folders"
        def subfolders_listing switch
            path = @conf.base_dir + "/" + switch
            folders = list_entries_from path
            folders.select! { |e| e.directory? }
             
        end

    end
end
