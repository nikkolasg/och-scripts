#!/usr/bin/ruby
##
## Read the different directories listed in the config.cfg file
## check if they exists, mkdir if not

require './config'

def exists_or_create dir
	print "Check directory #{Dir.pwd}/#{dir} ... "
	if !Dir.exists? dir
		Dir.mkdir dir
		print "created.\n"
	else
		print "checked.\n"
	end
end
## check if the dir is specified in config
def check_config dir
	dir_ = EMMConfig[dir]
    unless dir_
        $stderr.puts "No #{dir} entry specified in config file" 
        abort
    end
	dir_
end

#Check log 
log_ = check_config "LOG_DIR"
exists_or_create log_

#Check data dirs
data_ = check_config "DATA_DIR"
exists_or_create  data_

# check all subdirectories of data
# based on rule DATA_######_DIR
list_data_dir = EMMConfig.list_params.select { |p| p =~ /^DATA_(.*)_DIR$/ }.map {|d| EMMConfig[d] }
Dir.chdir data_
list_data_dir.each do |dir|
	exists_or_create dir
end

# check MSS subdirectories
list_mss_dir = EMMConfig.list_params.select { |p| p =~ /^MSS_SWITCHES_(.*)$/ }.map {|d| EMMConfig[d] }.flatten(1)
list_data_dir.each do |data_dir|
	Dir.chdir data_dir
	list_mss_dir.each do |mss_dir|
		exists_or_create mss_dir
	end
	Dir.chdir '..'
end

exit
