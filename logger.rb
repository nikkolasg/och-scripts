#!/usr/bin/ruby

class Logger

	
    require_relative 'config'
    @@log = Conf.logging
	def self.<<(script,level,msg)
        script = File.basename(script)
		log =  "#{Time.now.strftime("%d/%m/%y %H:%M")} #{script} : #{msg}" 
		if @@log.stdout  
           code = @@log.log_level.key(level)
           puts "#{level}\t #{log}" if code unless @@log.level_log > code
        end
		cmd = Conf::directories.app + "/do_alert.pl \"#{level}\" \"#{log}\""
		`#{cmd}`
	end
end

