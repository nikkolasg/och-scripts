module Getter
    require_relative '../ruby_util'
    require_relative '../util'
    require_relative './fetchers'
    require_relative '../database'
    require_relative '../logger'
    require_relative '../config'
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
        return {} if remote_files.empty?
        db = Database::Mysql.default
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
        db = Database::Mysql.default
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
            @opts_dir = infos[:dir]
            @folder = infos[:folder] ? infos[:folder] : File.dirname(infos[:files].first)
            @files = infos[:files].map { |f| CDR::File(::File.basename(f),@folder)} # just in case !
            @v = infos[:v]
            # try to guess the type
            @flow_name = Util.type(File.basename(@files.first))
            @switch = Util.switch(File.basename(@files.first))
            @flow = App.flow(@flow)
            raise "Unknown type file... " unless @flow
        end

        def get
            Util::starts_for(@opts_dir) do |dir|
                @dir = dir
                filter_files 
                return unless @files.size > 0
                move_files
                register_files_for_switch @switch, @files
            end
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
            manager = App::LocalFileManager.new
            new_dir = App.directories.store(@dir) + "/" + @switch
            manager.move_files @files, new_dir
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
            @opts_dir = infos[:dir]
            @v = infos[:v]
            @take = infos[:take] || nil
        end
        # just helper method to setup the right variable for
        # the current direction
        def setup 
            @sources = @flow.sources(@direction)
            @files = {}
        end

        def get
            Logger.<<(__FILE__,"INFO", "Starting GET operations in #{self.class.name}.." )
            ## make the operations for specified direction (including :both !!)
            Util::starts_for(@opts_dir) do |dir|
                @direction = dir
                setup 
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
            end
            Logger.<<(__FILE__,"INFO","GET Operation finished !")
        end

        private
        ## actually take the data to the app
        def download_files
            path = App.directories.tmp(@direction)
            @sources.each do |source|
                manager = source.file_manager
                manager.start do
                    source.switches.each do |switch|
                        files = @files[switch]
                        next if files.empty?
                        # download into the TMP directory by switch
                        spath = path + "/" + switch
                        manager.download_files files,spath
                        move_files switch
                    end
                end
            end
        end

        def move_files switch
            manager = App::LocalFileManager.new
            newp = App.directories.store(@direction) + "/" + switch
            manager.move_files @files[switch],newp
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
                str = "(DB)#{sw}: #{@files[sw].size} "
                @files[sw]= @files[sw].delete_if{|f|saved_files[sw].include? f.name}
                count_to_dl = count_to_dl + @files[sw].size
                str << "=> #{@files[sw].size}..."
                Logger.<<(__FILE__,"INFO",str)
            end

            return count_to_dl
        end

        ## LIST  the files on the host !
        def get_remote_files
            @sources.each do |source|
                manager = source.file_manager
                manager.start do 
                    source.switches.each do |switch|
                        @files[switch] = manager.find_files switch
                        @files[switch] = @files[switch].take(@take) if @take
                        Logger.<<(__FILE__,"INFO","Found #{@files[switch].size} files for #{source.base_dir}/#{switch} at #{source.host.address}")
                    end
                end
            end
        end

    end

end
