module Getter
    require_relative '../ruby_util'
    require_relative '../util'
    require_relative './fetchers'
    require_relative '../datalayer'
    require_relative '../logger'
    require_relative '../config/config'
    require_relative '../cdr'
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
        return if remote_files.empty?
        db = Datalayer::MysqlDatabase.default
        files = {}
        table = App.flow(@flow_name).table_cdr(@direction)
        db.connect do 
            # filter out according to "base name" (without .gz etc)
            query = "SELECT file_name,switch FROM #{table} WHERE file_name IN #{RubyUtil::sqlize(remote_files.map{|f|f.name})};"
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
                "( '#{f.name}','#{switch}' )"
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
            @files = infos[:files].map { |f| CDR::File(::File.basename(f),@folder)} # just in case !
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
            @files.delete_if do |file|
                db_files.include? file.name
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
            @sources = @flow.sources(@direction).map { |s| SourceHelper.new(s) }
            ## structure that contains the files
            # organized by switches ( hash)
            @files = @sources.inject({}) do |col,source|
                source.switches.each { |s| col[s] = [] }
                col
            end

            @v = infos[:v]
            (Logger.<<(__FILE__,"ERROR","No sources defined for this flow in that direction");abort;) unless @flow.sources(@direction).size > 0
        end

        def get
            Logger.<<(__FILE__,"INFO", "Starting GET operations in #{self.class.name}.." )
            get_remote_files
            count = filter
            if count == 0
                Logger.<<(__FILE__,"INFO","Filter out finished. Nothing to download!")
            else
                Logger.<<(__FILE__,"INFO","Filtering on remote files done ... will download #{count} files. ")
                download_files
                Logger.<<(__FILE__,"INFO","Files downloaded & moved into right folders ...")
                @files.each do |switch,list_files|
                    next if list_files.empty? # can be empty for some switches...
                    register_files_for_switch switch,list_files
                end
                Logger.<<(__FILE__,"INFO","Files registered into the system ! ")
            end
            Logger.<<(__FILE__,"INFO","GET Operation finished !")
        end

        private
        ## actually take the data to the app
        def download_files
            @sources.each do |source|
                # take only files for this source
                files_ = @files.select { |sw,f| source.switches.include? sw }
                source.download_files files_
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
            return f.size if saved_files.empty?
            #intersection of switches
            # limit the comparison between db and files
            # only filter by the switches retrieved from db
            switches = @files.keys & saved_files.keys
            return f.size if switches.empty?

            # THESE files wont have to be download
            # so we remove them from the global list
            switches.each do |sw|
                #puts @files[sw].sort {|f1,f2| f1.name <=> f2.name }.first.name + " ==> " + saved_files[sw].sort.first
                str = "(DB)#{sw}: #{@files[sw].size} "
                @files[sw]= @files[sw].delete_if{|f|saved_files[sw].include? f.name}
                #@files[sw] = @files[sw] - saved_files[sw]
                count_to_dl = count_to_dl + @files[sw].size
                str << "=> #{@files[sw].size}..."
                Logger.<<(__FILE__,"INFO",str)
            end

            return count_to_dl
        end
        # def list_to_download
        ### FIlter by files contained in SERVER folder too !
        ##file NOT contained in the server, will have to be downloaded
        ## SO we create a list of files to download, which is by definition
        ## also contained in the global list (since .clone)
        #count_to_dl = 0
        #path = App.directories.store(@direction)
        #local_fetch = Fetchers::create(:LOCAL,{})
        #files_to_dl = @files.clone
        #files_to_dl.each do |switch,list_files|
        #_path = "#{path}/#{switch}"
        ## get files already downloaded for theses switches
        #files_stored = local_fetch.list_files_from _path   
        ## simple out put
        #str = "(LOCAL) #{switch}: #{list_files.size} "

        #files_to_dl[switch] = list_files - files_stored
        #count_to_dl = count_to_dl + files_to_dl[switch].size

        #str << " => #{files_to_dl[switch].size}..."
        #Logger.<<(__FILE__,"INFO",str)
        #end
        #return files_to_dl,count_to_dl
        #end

        def get_remote_files
            @sources.each do |source|
                @files.merge! source.list_files
            end
        end

    end

    # represent a "source"
    # middle class that handles the creation of fetcher
    # the listing of files 
    # and the downloading !
    # very usefule for a clean code because of the complexity of
    # the different sources => cdr into multiple subfolders etc etc
    class SourceHelper

        def initialize(configSource)
            @conf = configSource
            @data = Hash[@conf.switches.map {|s| [s,[]]}]
            @fetcher = Fetchers::create(@conf.protocol,@conf.host,@conf.login,@conf.password)
        end
        #simple proxy method
        def switches
            return @conf.switches
        end
        # return an hash of files present on the source
        # key : switch
        # values : files
        def list_files
            @fetcher.connect do 
                @conf.switches.each do |switch|
                    if !@conf.sub_folders ## no subfolders
                        simple_listing switch
                    else ## subfolder ...
                        subfolders_listing switch
                    end
                end
            end
            return @data
        end

        def download_files files
            @data = files
            @fetcher.connect do 
                @data.keys.each do |switch|
                    next if @data[switch].empty? 
                    path = @conf.base_dir + "/" + switch
                    local_p = App.directories.tmp(@conf.direction) + "/" + switch
                    if !@conf.sub_folders
                        files_list = @data[switch].map {|f|f.cname}
                        @fetcher.download_files local_p,path,files_list
                    else
                        dls = []
                        @data[switch].each do |file|
                            # download the file as-is in local 
                            dls << @fetcher.download_file((local_p+"/"+file.cname),file.full_path)
                        end 
                        # asynchronous
                        dls.each { |d| d.wait }
                    end
                    move_into_store switch,@data[switch]
                end
            end
        end

        private

        def move_into_store switch,files
            f = Fetchers::create(:LOCAL,{})
            new_p = App.directories.store(@conf.direction) + "/" + switch
            old_p = App.directories.tmp(@conf.direction) + "/" + switch

            files_list = files.map{|f| f.cname}
            f.download_files new_p, old_p, files_list
        end


        # simply list all files directly from the different switches
        def simple_listing switch
            path = @conf.base_dir + "/" + switch
            files = @fetcher.list_files_from(path,regexp: @conf.regexp)
            @data[switch] = files.map{ |f| CDR::File.new(f,path) }
        end 

        # list all files contained in sub folders.
        # Will take the N first folders specified in 
        # the conf as "sub_folders"
        def subfolders_listing switch
            path = @conf.base_dir + "/" + switch
            folders =@fetcher.list_files_from path,directories: true
            folders = folders.take(@conf.sub_folders)
            folders.each do |folder|
                fo = File.basename(folder)
                sub_ = path + "/" + fo
                files  = @fetcher.list_files_from sub_,regexp: @conf.regexp
                ## here canonical name is required
                #so when we actually want to DL the data, we use the sub folders!
                @data[switch] = files.map {|fi| CDR::File.new(fi,sub_)}
            end
        end

    end
end
