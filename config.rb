#!/usr/bin/ruby 

## Config file reader
## automatically handles the multiple values into an array
class EMMConfig
	require 'parseconfig'

	PATH = "config.cfg"
	@@C = {}
	def self.parse
		@@C = ParseConfig.new(PATH)	
	end

	def self.[](param)
		val = @@C[param]
		if val && val.include?(",")
		 	val.split(',')
		else
			val
		end
	end

	def self.list_params
		@@C.get_params
	end

		
	parse
end



