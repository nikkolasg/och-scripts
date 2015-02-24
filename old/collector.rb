#!/usr/bin/ruby
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
## Collect datas from different source
## and put them in the database
## i.e. is given multiple fetchers
## and one database
## will fetch the files and put in the 
## database only those who are new
## comparison is only done by the file name
## (and by processed field also)
require 'set'
require './logger'
require './config'
require './fetchers'
require './database'
require './util'

class Collector
		
	# fetchers is a special hash
	# key : hosts
	# value : hash =>	
	#	[:fetcher] = the created fetcher for this host
	# 	[:switches] = arr of switches on this host
	#	[:base_path] = the base path on this host

	def initialize(table,fetchers)
		@table = table
		@fetchers = fetchers
	end
	
	## main method
	## opts may contains specific search fields
	## aside file_name, + others-.--
	## example :
	## opts = { 	where: { processed:"0" } ,
	##		down_dir: "data/backlog" }
	def collect opts = {}
		new_files = {}
		@fetchers.each do |host,val|
			new_files[host] = fetch_files_from val	
		end

		files_to_keep = get_stored_files new_files, opts

		count = store_new_files files_to_keep, opts

		Logger.<<($0,"INFO","Collected #{count} files in total !")
	end
	## download all the enw files, and put it in db
	## TODO verify the speed of this
	## maybe download ALL FILES in ONE batch
	## here, download from all files from one directory at a time
	def store_new_files new_files , opts={}
		count = 0
		@fetchers.each do |host,val|
			fetcher = val[:fetcher]
			base_path = val[:base_path]
			# custom download path
			down_dir = opts[:down_dir].nil? ? EMMConfig['DATA_DOWN_DIR'] : opts[:down_dir]
			down_dir = Util.data_path(down_dir)
			fetcher.connect do
			## The download it self
			new_files[host].each do |sw,list|
				next if list.size == 0
				local_dir = "#{down_dir}/#{sw}"
				remote_dir ="#{base_path}/#{sw}"
				fetcher.download_files(local_dir,remote_dir,list)
				
				Logger.<<($0,"INFO","Downloaded #{list.size} files from #{sw}")

				## since nothing went wrong we can move them
				store_dir = opts[:store_dir].nil? ? EMMConfig['DATA_STORE_DIR'] : opts[:store_dir] 
				store_dir = Util.data_path(store_dir)
				cmd = "mv -t #{store_dir}/#{sw} #{list.map{|f| local_dir +"/"+ f }.join(' ')} 2>&1"
				res = `#{cmd}`	
				if !res.empty? 
					Logger.<<($0,"ERROR","while executing the mv command: res=#{res} \ncmd = #{cmd}")
					raise "Error during the mv command"
				end
				Logger.<<($0,"INFO","Moved #{list.size} files to  #{store_dir}/#{sw}")

				## insert in db
				h = { fields: ["file_name","switch"] }
				h[:values] = list.map {|f| [f,sw]}
				if !@table.insert_multiple  h
                    Logger.<<($0,"ERROR","Error during database transaction. Aborting.")
                    raise "Database transaction error. Abort."
                end
				Logger.<<($0,"INFO","Registered #{list.size} files in #{@table.table_name} ")
				count = count + list.size
			end
			end
		end
		count
	end

	## fetch files from host in every path specified
	def fetch_files_from host
		fetcher = host[:fetcher]
		switches = host[:switches]
		base_path = host[:base_path]
		list_files = {}
		fetcher.connect do
		switches.each do |s|
			path = base_path + "/" + s.to_s + "/"
			list_files[s] = fetcher.list_files_from path
		end
		end
		list_files
	end
	## retrieve from the DB the files already processed / fetch
	## from the list of new files
	## transform the new_files to only have the new
	def get_stored_files new_files, opts = {}
		#prepare the query
		select = ["*"]
		select = ops[:select] if opts[:select]

		flat_files = new_files.map {|h,v| v.map { |sw,list| list.to_a }.flatten(1) }.flatten(1)
		where = { file_name: flat_files }
		where.merge opts[:where] if opts[:where]
		
		#make the query
		res = @table.search_and(select,where)
		# just transform result in a more flexible manner	
		alreadyStored = Set.new
		res.each_hash do |row|
			alreadyStored << row['file_name']
		end
		Logger.<<($0,"INFO","Retrieved #{alreadyStored.size} file names from database ...")		
		## conpute the difference
		## and transform into array for easier manipulation
		## (Set is fast for difference)
		count = 0
		files_to_keep = new_files.clone
		new_files.each do |h,sws|
			sws.each do |sw,list|
			    files_to_keep[h][sw] = (new_files[h][sw].subtract alreadyStored).to_a
			    count = count + new_files[h][sw].size
			end
		end
		Logger.<<($0,"INFO","Will keep only #{count} files among retrieved")
		files_to_keep
	end

end