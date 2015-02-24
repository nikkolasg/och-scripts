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