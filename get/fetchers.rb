module Fetchers
require_relative '../logger'
require 'io/wait'

# Multiple definitions of classes / factory 
# to easily retrieve raw cdr in case the source change 
# and method of retrieval also
# i.e. either by database, by sftp, or by local files etc
	@@subclasses = {}
	
	def self.create (type, *args)
		(Logger.<<(__FILE__,"ERROR","No options given for the raw cdr fetcher");raise "Error";) unless args.size > 0
		c = @@subclasses[type]
		if c
			c.new(*args)
		else
			raise "Bad fetcher type #{type}, not in #{@@subclasses.inspect}"
		end
	end
    
    # fetchres class must register here
    # so they can be instantiated by Fetchers.create
	def self.register_reader name,class_name
		@@subclasses[name] = class_name
	end
class SftpFileFetcher 
	require 'net/sftp'
	require 'set'
	# register to the fetchers module so it can be isntantiated
	Fetchers.register_reader :SFTP,self
	protected_methods :new	
    attr_reader :host
	def initialize(host,login,password,regexp = nil)	
		@host = host
		@login =login
		@password = password
		@regexp = regexp ? regexp : "*.DAT"
		@sftp = nil
	end
	## MAIN METHOD , everything must pass inside this method
	def connect
		begin
		Net::SFTP.start(@host,@login,password:@password) do |sf|
			@sftp = sf
			Logger.<<(__FILE__,"INFO","Connected at #{@login}@#{@host}")
			yield
		end
		rescue => e
			Logger.<<(__FILE__,"ERROR",e.message)
			raise e
		end	
        Logger.<<(__FILE__,"INFO","Disconnected from #{@login}@#{@host}")
        @sftp = nil	
	end
	# list all files from the given path (SINGLE PATH)
	def list_files_from (path)
		safe_fetch do
		list_files = Set.new
		var = "Search files in #{path} with #{@regexp} at #{@host}... "
		@sftp.dir.glob(path,@regexp) do |entry|
			list_files << entry.name
		end
		var << "Found #{list_files.size}\n"
		Logger.<<(__FILE__,"INFO",var)
		list_files
		end
	end

    def list_entries_from path
        entries = []
        safe_fetch do 
            @sftp.dir.entries(path)
        end
    end
	
	# download a file
	def download_file (remote_path,local_path)
		safe_fetch do
			@sftp.download!(remote_path, local_path)
		end
	end
	## download multiple files from a directory in a batch
	## local dir is the dir where to download the file on the local machine
	## remote_paths is the dir where to get the files on the remote machine
	## remote_files is an arr of files names to get
	def download_files local_dir, remote_dir,remote_files,opts = {}
		safe_fetch do
        Logger.<<(__FILE__,"INFO","Will start download #{remote_dir}/* from #{@host} to #{local_dir}...")
		dls = remote_files.map do |remote_file|
			local_path = "#{local_dir}/#{remote_file}"
			@sftp.download("#{remote_dir}/#{remote_file}",local_path)
		end
		dls.each {|d| d.wait}
        Logger.<<(__FILE__,"INFO","Downloaded #{dls.size} files from #{remote_dir} at #{@host}")
		end
	end
	# just a wrapper so exception are caught
	def safe_fetch
		Logger.<<(__FILE__,"ERROR","SFTP not connected (must use in 'connect' method)") unless @sftp 
		raise "SFTP not connected (must use in 'connect' method)"  unless @sftp
		begin
			yield
		rescue => e
			Logger.<<(__FILE__,"ERROR",e.message)
			raise e
		end
	end
end	

class LocalFileFetcher 
    Fetchers.register_reader :LOCAL,self
    def initialize(opts = {})
        @opts = opts
    end
    def list_files_from path
       unless Dir.exists? path
           Logger.<<(__FILE__,"ERROR","Local fetcher: path does not exists for listing... #{path} ")
           raise "Error LocalFileFetcher !"
       end
       cmd = "ls #{path}"
       out = `#{cmd}`
       return out.split("\n")
    end
    

    def download_files new_dir,old_dir,remote_files, opts = {}
        unless (Dir.exists?(new_dir) && Dir.exists?(old_dir))
            Logger.<<(__FILE__,"ERROR","Local fetcher download : wrong dir arguments #{old_dir} => #{new_dir}")
            raise "Error LocalFileFetcher"
        end
        cmd = "mv -t #{new_dir} "
        cmd << remote_files.map{ |f| old_dir + "/" + f }.join(" ")
        error = nil
        Open3.popen3(cmd) do |stdin,stdout,stderr,thr|
            error = stderr.read if (stderr.ready? &&!thr.value.success?)
        end
        if error
            Logger.<<(__FILE__,"ERROR","Local Fetcher: error while mv #{error}")
            raise "Error LocalFileFetcher"
        end
    end

    def delete_files_from dir
       Dir[dir+"/*"].each do |file|
          delete_file file
       end 
    end

    def delete_file file
        unless File.exists? file
            Logger.<<(__FILE__,"ERROR","FileFetcher can not delete a non existant file ! #{file}")
            abort;
        end
        cmd = "rm #{file}"
        unless system(cmd)
            Logger.<<(__FILE__,"ERROR","FileFetcher trouble for deleting file #{file}")
            abort
        end
    end
end

end
