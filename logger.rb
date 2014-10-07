#!/usr/bin/ruby

class Logger

	require './config'
	TO_STDOUT= EMMConfig['STDOUT'] ? EMMConfig["STDOUT"] : nil
	def self.<<(script,level,msg)
        script = File.basename(script)
		log =  "#{Time.now} #{script} : #{msg}" 
		puts "#{level}\t #{log}" if TO_STDOUT
		cmd = "do_alert.pl \"#{level}\" \"#{log}\""
		`#{cmd}`
	end

end

