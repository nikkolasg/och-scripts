#
# Copyright (C) 2014-2015 Nicolas GAILLY for Orange Communications SA, Switzerland
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
module Getter
    require_relative '../ruby_util'
    require_relative '../util'
    require_relative '../database'
    require_relative '../logger'
    require_relative '../config'
    require_relative '../cdr'
    require 'open3'
    require 'json'


    ## LIST  the files on the host !
    def get_remote_files
        manager = @current_source.file_manager
        manager.start do 
            @current_source.folders.each do |folder|
                @files[folder] = manager.find_files folder
                @files[folder] = @files[folder].take(@take) if @take
                Logger.<<(__FILE__,"INFO","Found #{@files[folder].size} files for #{@current_source.base_dir}/#{folder} at #{@current_source.host.address}")
                SignalHandler.check
            end
        end
    end

    ## filter the files to get
    # by the db and thoses already downloaded
    # (sometimes useful to testing multiple times )
    # return the number of files to download
    def filter 
        # get files contained in the list and also in db
        # ==> files to eliminate
        count = 0
        f = @files.values.flatten(1)
        saved = @current_source.schema.filter_files f
        @files.keys.each do |folder|
            files = @files[folder]
            ocount  = files.size
            files -= saved
            ncount = files.size
            @files[folder] = files
            Logger.<<(__FILE__,"INFO","Filtering (#{folder}) : #{ocount} => #{ncount}")
            count += ncount
        end
        return count
    end

    class NullSourceGetter
        include Getter

        def initialize(source,infos)

        end

        def get
            Logger.<<(__FILE__,"INFO","Get operation on NULL getter ... Already done !!")
        end
    end

    
    # responsible for handling the flows get operations
    class GenericSourceGetter
        include Getter

        def initialize(source,infos)
            @current_source = source
            @v = infos[:v]
            @take = infos[:take] || nil
            @files = {}
            @opts = infos
            @current_source.set_options(@opts)
        end

        def get
            Logger.<<(__FILE__,"INFO", "Starting GET operations in #{self.class.name} for #{@current_source.name}.." )
            get_remote_files
            count = filter
            if count == 0
                Logger.<<(__FILE__,"INFO","Filter out finished. Nothing to download!")
            else
                Logger.<<(__FILE__,"INFO","Filtering on remote files done ... will download #{count} files. ")
                download_files
                Logger.<<(__FILE__,"INFO","Files downloaded & moved & registered in the systems ! ...")
            end
            @files = {}
            Logger.<<(__FILE__,"INFO","GET Operation finished !")
        end

        private
        ## actually take the data to the app
        def download_files
            path = Conf::directories.tmp
            manager = @current_source.file_manager
            manager.start do
                @current_source.folders.each do |folder|
                    files = @files[folder]
                    puts "folder => #{folder}, Files => #{files}" if @opts[:v]
                    next if files.empty? 
                    # download into the TMP directory by folder
                    spath = File.join(path, @current_source.name.to_s,folder)
                    manager.download_files files,spath,v: true
                    move_files folder
                    @current_source.schema.insert_files files,folder
                    SignalHandler.check { Logger.<<(__FILE__,"WARNING","SIGINT Catched. Abort.")}
                end
            end
        end

        def move_files folder
            manager = Conf::LocalFileManager.new
            newp = File.join(Conf::directories.store,@current_source.name.to_s,folder)
            manager.move_files @files[folder],newp
        end


    end

end