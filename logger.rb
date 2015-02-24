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