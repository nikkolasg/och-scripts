#!/usr/bin/ruby 

## Config file reader
## automatically handles the multiple values into an array
class EMMConfig
    require 'parseconfig'

    PATH = "config.cfg"
    @@C = {}
    def self.parse_files
        @@C.get_params.select do |param|
            next unless param =~ /(\w+)_(\w+)_FIELDS_FILE/
            flow = $1
            type = $2 # cdr, record or stat
            File.open(@@C[param],"r") do |file|
                fields = []
                p = flow + "_" + type + "_" + "FIELDS"
                file.each_line  do |line|
                    value = line.split(":")[0]
                    fields << value
                end
                @@C.add(p,fields.join(','))
            end
        end
    end

    def self.parse
        @@C = ParseConfig.new(PATH)	
        EMMConfig.parse_files
    end
    def self.[](param)
        val = @@C[param]
        if val && val.include?(",")
            val.split(',')
        elsif val
            val.dup
        end
    end

    def self.list_params
        @@C.get_params
    end


    parse
end



