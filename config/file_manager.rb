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
module Conf
    require_relative '../cdr'
    require_relative '../ruby_util'
    ## class that will handle all operations for files
    # on remote/local host for the GET operations and others
    class FileManager
        # name that can be found with -name option of FIND GNU util
        FILE_REGEXP = '.*\.gz|.*\.DAT\.gz|.*\.DAT|.*\.dat|.*\.DAT\.GZ|.*\.dat\.gz|.*\.dat\.GZ'
        # regex taht can be used with -regextype posix-basic -regex of Find GNU util
        #                     YEAR    MONTH   DAY
        FOLDER_REGEXP = %(.*/[0-9]{4}[0-9]{2}[0-9]{2})
        # from wichi min_date to list/download the files !
        # syntax from the DATE GNU Util
        TODAY = "today"

        @@subclasses = {}
        def self.register_subclass protocol
            @@subclasses[protocol] = self
        end

        # simple factory for getting files from multiple host
        # with multiple protocol
        # can also create LocalFileManager for basic files operation
        # directly with the constructor
        def self.create(source,opts = {})
            c = @@subclasses[source.host.protocol]
            unless c
                Logger.<<(__FILE__,"ERROR","Unknown File Manager protocol ")
                abort
            end
            c.new(source,opts)
        end


        attr_accessor :file_regexp,:folder_regexp
        attr_accessor :subfolders # switch separates their files into subfolders
        attr_reader :min_date,:min_date_filter # date limit to retrieve files
        attr_reader :max_date,:max_date_filter
        attr_accessor :take

        def initialize(source,opts = {})
            @source = source
            @host = source.host
            @file_regexp = FILE_REGEXP
            @folder_regexp = FOLDER_REGEXP
            @subfolders = true
            self.send(:min_date=, TODAY)
            self.send(:min_date=, TODAY)
            @started = false # boolean to know if operations have started or not
            @v = opts[:v]
            @opts = opts
        end
        # in case we want to change some things after being initialized
        # best to go by this method, so every attr is consistent
        def set_options(opts = {})
            self.send(:min_date=,opts[:min_date]) if opts[:min_date]
            self.send(:max_date=,opts[:max_date]) if opts[:max_date]
            @subfolders = opts[:subfolders] if opts[:subfolders]
            @file_regexp = opts[:file_regexp] if opts[:file_regexp]
            @folder_regexp = opts[:folder_regexp] if opts[:folder_regexp]
        end
        # so we automatically have the
        # string corresponding to the min_date 
        def min_date=(d)
            @min_date = d
            @min_date_filter = date_str @min_date
        end

        def max_date=(d)
            @max_date = d
            @max_date_filter = date_str @max_date
        end

        ## General function to call 
        #will find all files according to spec (regex,date etc)
        #will automatically handle the transition between subfolder and not
        #return an array of CDR::File
        def find_files folder
            unless @started
                Logger.<<(__FILE__,"ERROR","FileManager is not started yet !")
                abort
            end
            if @subfolders
                files = sub_listing folder
            else
                path = ::File.join(@source.base_dir,folder)
                files =  files_listing path
            end
            files = files_filtering files
            files = files.take(@take) if @take
            to_file(files)
        end

        # expect a array of CDR::Files, 
        # and a string representing the destination folder
        def download_files files,dest,opts = {}
            unless @started
                Logger.<<(__FILE__,"ERROR","FileManager is not started yet !")
                abort
            end
            str = "Will download #{files.size} files to #{dest} ... "
            download_all_files files,dest
            str += " Done !"
            Logger.<<(__FILE__,"INFO",str) if opts[:v]
        end

        protected 


        # return a list of CDR::File objects
        def to_file files
            files.map { |f| CDR::File.new(f) }
        end

        # list all files in a path
        def files_listing path
            cmd = "find #{path} -type f "
            cmd += "-regextype posix-extended "
            cmd += "-regex \"#{@file_regexp}\" " if @file_regexp
            out = exec_cmd(cmd)
        end

        # make a fist attempt at listing the folder,
        # filter them out, and then do the listing for each of them
        def sub_listing switch
            path = ::File.join(@source.base_dir,switch)
            folders = folders_listing path
            folders = folders_filtering folders
            files = []
            folders.each do |folder|
                files += files_listing folder
            end
            files
        end

        ## list all folders corresponding to spec in this path
        def folders_listing path
            cmd = "find #{path} -type d "
            if @folder_regexp
                cmd += "-regextype posix-extended "
                cmd += "-regex \"#{@folder_regexp}\""
            end
            folders = exec_cmd(cmd)
            folders
        end

        # filter the files by the spec of this filemanager
        def files_filtering files
            return files unless @file_regexp
            f = files.select do |file|
                test_name_by_date file
            end
            f
        end

        # filter the folders by the spec (mostly date for now)
        def folders_filtering folders
            return folders unless @folder_regexp
            folders.select do |folder|
                test_name_by_date folder
            end
        end

        # compute the string date specified for this filemanager
        def date_str d
            Util::date(d)
        end

        def test_name_by_date name
            f = File.basename(name)
            ret = Util::decompose f,:day
            y,m,d = ret
            ret ? test_date("#{y}#{m}#{d}") : false
        end

        # return true of false for a certain date
        # regarding the spec of this file manager
        def test_date date
            res = true
            res = res && @min_date_filter <= date if @min_date_filter
            res = res && @max_date_filter >= date if @max_date_filter
            res
        end

        ## use only for signal processing handling (SIGINT)
        def stop
            Logger.<<(__FILE__,"INFO","Exiting file manager gracefully ...")
        end
    end

    class SftpFileManager < FileManager
        self.register_subclass :sftp

        require 'net/sftp'
        require 'net/ssh'

        def initialize(source,opts = {})
            super(source,opts)
            @ssh = nil
            @sftp = nil
        end

        def start 
            begin
                @ssh = Net::SSH.start(@host.address,@host.login,password:@host.password) 
                @started = true
                Logger.<<(__FILE__,"INFO","Connected at #{@host.login}@#{@host.address}")
                yield
                @ssh.close
            rescue => e
                Logger.<<(__FILE__,"ERROR",e.message)
                raise e
            end	
            Logger.<<(__FILE__,"INFO","Disconnected from #{@host.login}@#{@host.address}")
            @sftp = nil	
        end

        ## use only for signal handling processing.
        #
        def stop
            @ssh.close if @ssh
            super.stop
        end
        # complex command so we can see if any errors occured...
        def exec_cmd cmd
            t = Time.now
            results = ""
            @ssh.open_channel do |channel|
                channel.exec(cmd) do |ch,success|
                    unless success
                        Logger.<<(__FILE__,"INFO","Could Not execute command #{cmd}")
                        abort
                    end
                    # stdout
                    channel.on_data do |ch,data|
                        results += data
                    end
                    # stderr
                    channel.on_extended_data do |ch,type,data|
                        Logger.<<(__FILE__,"ERROR","Error from the cmd #{cmd} : #{data}")
                        abort
                    end
                    channel.on_close do |ch|
                    end
                end
            end
            # wait for the command to finish
            @ssh.loop
            Logger.<<(__FILE__,"DEBUG","SFTP Command executed in #{Time.now - t} sec") if @opts[:v]
            results.split
        end

        #download all files into one dest
        def download_all_files files,dest,opts = {}
            @ssh.sftp.connect do |sftp|
                ## in case we have too much files !!
                RubyUtil::partition files do |sub|
                    dl = []
                    sub.each do |file|
                        dest_file = ::File.join(dest,file.cname)
                        dl << sftp.download(file.full_path,dest_file)  
                    end
                    dl.each { |d| d.wait }
                end
            end
            files.each { |f| f.path = dest; f.downloaded = true }
            files
        end
    end

    class LocalFileManager < FileManager
        self.register_subclass :local

        def initialize(source,opts = {})
            super(source,opts)
            @started = true
        end

        # so we can instantiate it directly for files operation maintenance
        def initialize()
        end

        # simple proxy so same utilization between all filemanager
        def start
            yield
        end

        def exec_cmd cmd
            out,err,s = Open3.capture3(cmd)
            if !s.success?
                Logger.<<(__FILE__,"ERROR","LocalFileManager : exec cmd #{err} => \n#{cmd}")
                abort
            end
            out.split
        end

        # does what the name suggests !
        def ls dir,regex = ""
            cmd = regex.empty? ? "ls #{dir}" : "ls #{File.join(dir,regex)}"
            exec_cmd(cmd) 
        end

        def delete_dir dir
            cmd = "rm -rf #{dir}"
            exec_cmd(cmd)
        end

        def rename_dir dir,ndir
            cmd = "mv #{dir}  #{ndir}"
            exec_cmd(cmd)
        end

        # download all FILES object into the dest path
        # update the files path !!
        def download_all_files files,dest
            return if files.empty?
            RubyUtil::partition files do |sub|
                cmd = "mv -t #{dest} "
                cmd += sub.map { |f| "\"#{f.full_path}\"" }.join(' ')
                exec_cmd(cmd) 
            end
            # update files object !
            files.map! { |f| f.path = dest; f }
        end
        alias :move_files :download_all_files

        ## Delete all files from the directory name
        def delete_files_from dir
            Dir[dir+"/*"].each do |file|
                File::delete(file)
            end 
        rescue => e
            Logger.<<(__FILE__,"WARNING","Files from #{dir} could not be deleted.\n#{e.message}")
        end

        ## Delete CDR::File object
        def delete_files *files
            files.each do |file|
                unless File.exists? file.full_path
                    Logger.<<(__FILE__,"ERROR","LocalFileManager can not delete a non existant file ! #{file.cname}")
                    abort;
                end
                cmd = "rm '#{file.full_path}'"
                unless system(cmd)
                    Logger.<<(__FILE__,"ERROR","LocalFileManager trouble for deleting file #{file}")
                    abort
                end
            end
        end

        ## Delete the backup  & store for this source !!
        def delete_source_files source
            dir = Conf::directories
            base = File.join(dir.data)
            source.folders.each do |fold|
                url = File.join(dir.store,source.name.to_s,fold)
                delete_files_from url
                url = File.join(dir.backup,source.name.to_s,fold)
                delete_files_from url
            end
                Logger.<<(__FILE__,"INFO","Deleted files server & backup for #{source.name.to_s}")
        end

      ##
       # restore the backup files for this source.
        # i.e. puts them into the server folder
        def restore_source_files source
            dir = Conf::directories
            base = File.join(dir.data)
            source.folders.each do |fold|
                url = File.join(dir.backup,source.name.to_s,fold)
                files = ls(url).map { |f| CDR::File.new(::File.join(url,f),search: true)}
                nurl = File.join(dir.store,source.name.to_s,fold)
                download_all_files(files,nurl) unless files.empty?
            end
            Logger.<<(__FILE__,"INFO","Restored file from backup & server for #{source.name.to_s}")
        end

        def rename_source_files source,nname
            dir = Conf::directories
            base = dir.data
            [:tmp,:store,:backup].each do |fold|
                ourl = File.join(dir.send(fold),source.name.to_s)
                nurl = File.join(dir.send(fold),nname)
                rename_dir ourl,nurl
            end
        end
    end

    class FileManager

        class << self 
            def test(protocol)
                # get the first source with this protocol
                source = get_protocol_source protocol
                unless source
                    Logger.<<(__FILE__,"ERROR","FileManager Test did not find any sources with the protocol required")
                    abort
                end
                manager = FileManager::create(source,v: true)
                manager.instance_eval do 
                    manager.start do 
                        source.switches.each do |switch|
                            path = File::join(@source.base_dir, switch)
                            folders = folders_listing path
                            FileManager::print_listing folders,"Folder Listing"
                            filtered_folders = folders_filtering folders
                            FileManager::print_listing filtered_folders,"Folders Filtering"
                            filtered_folders.each do |folder|
                                files = files_listing folder
                                FileManager::print_listing files,"Files Listing for #{folder}"
                                filtered_files = files_filtering files
                                FileManager::print_listing files,"Files Filtered for #{folder}"
                            end
                        end
                    end
                end

            end



            def get_protocol_source(protocol)
                source = nil
                App.flows.each do |flow|
                    flow.sources.each do |source_|
                        next if source_.host.protocol != protocol
                        Logger.<<(__FILE__,"INFO","FileManager:test have found source #{source_.name} for the specified protocol")
                        source = source_
                        break
                    end
                    break if source
                end
                source
            end 

            def print_listing list,operation
                str = operation + " : "
                str += "found #{list.size} elements...\n"
                Logger.<<(__FILE__,"INFO",str)
            end
        end

    end
end