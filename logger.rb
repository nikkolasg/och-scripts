#!/usr/bin/ruby

class Logger

	require './config/config.rb'
	
	def self.<<(script,level,msg)
        script = File.basename(script)
		log =  "#{Time.now} #{script} : #{msg}" 
		puts "#{level}\t #{log}" if App.logging.stdout
		cmd = "do_alert.pl \"#{level}\" \"#{log}\""
		`#{cmd}`
	end
end

