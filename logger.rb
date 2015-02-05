#!/usr/bin/ruby

class Logger

	
    require_relative 'config'
    @@log = Conf.logging
    @@inline = false
    STDOUT.sync = true

	def self.<<(script,level,msg,opts = {})
        script = File.basename(script)
		log =  "#{Time.now.strftime("%d/%m/%y %H:%M")} #{script} : #{msg}" 
		if @@log.stdout  
           code = @@log.log_level.key(level)
           print("\n") if @@inline && !opts[:inline]  ## finished inline editing
           ## either new line or not if we want inline cmd line editing
           meth = opts[:inline] ? method(:print) : method(:puts)
           meth.call("\r#{level}\t #{log}") if code unless @@log.level_log > code
           @@inline = opts[:inline]
        end
		cmd = Conf::directories.app + "/do_alert.pl \"#{level}\" \"#{log.gsub("\r","")}\""
		`#{cmd}`
	end
end

