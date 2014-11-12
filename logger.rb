#!/usr/bin/ruby

class Logger

	
    require './config'
    @@log = App.logging
	def self.<<(script,level,msg)
        script = File.basename(script)
		log =  "#{Time.now.strftime("%d/%m/%y %H:%M")} #{script} : #{msg}" 
		if @@log.stdout  
           code = @@log.log_level.key(level)
           puts "#{level}\t #{log}" if code unless @@log.level_log > code
        end
		cmd = "do_alert.pl \"#{level}\" \"#{log}\""
		`#{cmd}`
	end
end

