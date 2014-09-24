module Fetchers
require './logger'
# Multiple definitions of classes / factory 
# to easily retrieve raw cdr in case the source change 
# and method of retrieval also
# i.e. either by database, by sftp, or by local files etc
class FileFetcher
	@@subclasses = {}
	
	def self.create type, options = nil
		Logger.<<($0,"ERROR","No options given for the raw cdr fetcher") unless options
		raise "No options given for the raw cdr fetcher" unless options
		c = @@subclasses[type]
		if c
			c.new(options)
		else
			raise "Bad fetcher type #{type}, not in #{@@subclasses.inspect}"
		end
	end

	def self.register_reader name
		@@subclasses[name] = self
	end
end


class SftpFileFetcher < FileFetcher
	require 'net/sftp'
	require 'set'
	# register to the mother class so it can be isntantiated
	register_reader :sftp
	protected_methods :new	

	def initialize(options)	
		@host = options[:host].to_s
		@login = options[:login].to_s		
		@password = options[:pass] if options.has_key? :pass
		@regexp = options.has_key?(:regexp) ? options[:regexp] : "*.DAT"
		@sftp = nil
	end
	## MAIN METHOD , everything must pass inside this method
	def connect
		begin
		Net::SFTP.start(@host,@login,password:@password) do |sf|
			@sftp = sf
			Logger.<<($0,"INFO","Connected at #{@login}@#{@host}")
			yield
		end
		rescue => e
			Logger.<<($0,"ERROR",e.message)
			raise e
		end	
	
	end
	# list all files from the given path (SINGLE PATH)
	def list_files_from (path)
		safe_fetch do
		list_files = Set.new
		var = "Search files in #{path} with #{@regexp}... "
		@sftp.dir.glob(path,@regexp) do |entry|
			list_files << entry.name
		end
		var << "Found #{list_files.size}\n"
		Logger.<<($0,"INFO",var)
		list_files
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
	def download_files local_dir, remote_dir,remote_files
		safe_fetch do
		dls = remote_files.map do |remote_file|
			local_path = "#{local_dir}/#{remote_file}"
			@sftp.download("#{remote_dir}/#{remote_file}",local_path)
		end
		dls.each {|d| d.wait}
		end
	end
	# just a wrapper so exception are caught
	def safe_fetch
		Logger.<<($0,"ERROR","SFTP not connected (must use in 'connect' method)") unless @sftp 
		raise "SFTP not connected (must use in 'connect' method)"  unless @sftp
		begin
			yield
		rescue => e
			Logger.<<($0,"ERROR",e.message)
			raise
		end
	end
end	

class DatabaseFileFetcher < FileFetcher

	register_reader :database

	def initialize(options = {})
	end

	def list_files
	end
end
end
