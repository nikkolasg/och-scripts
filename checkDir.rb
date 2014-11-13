#!/usr/bin/ruby
##
## Read the different directories listed in the config.cfg file
## check if they exists, mkdir if not

require './config'

def exists_or_create dir
	str =  "Check directory #{dir} ... "
	if !Dir.exists? dir
		Dir.mkdir dir
		str <<  "created.\n"
	else
		str << "checked.\n"
	end
    Logger.<<(__FILE__,"INFO",str)
end

dirs = App.directories
exists_or_create dirs.data
flows = App.flows
[:tmp,:store,:backup].each do |f|
    instance_eval("exists_or_create dirs.#{f}")
    instance_eval("exists_or_create dirs.#{f}(:output)")
    flows.each do |flow|
        path = instance_eval("App.directories.#{f}") 
        path_out = instance_eval("App.directories.#{f}(:output)")
        flow.switches.each do |sw|
            str = "exists_or_create \"#{path}/#{sw}\""
            str2 = "exists_or_create \"#{path_out}/#{sw}\""
            instance_eval(str)
            instance_eval(str2)
        end
    end
end

log = App.logging
exists_or_create log.log_dir

