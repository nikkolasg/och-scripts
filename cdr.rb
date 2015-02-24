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
#
#just a module to wrap a custom File obj
#used trhoughout the tool
#end
module CDR
    require_relative'config'
    require 'open3'
    require_relative 'util'
    require 'pry'
    # helper class to facilitate the 
    # different format of cdr, and compressed or not ,
    # stored in subfolder ( ==> cname) etc.
    class File
        ## cname is the "real"  name
        #  name is just the name of the cdr, without any others extensions
        attr_accessor :name,:cname,:full_path,:downloaded

        def initialize(name,opts = {})
            if opts[:nosearch]
                @name = remove_ext(name) ## only record the name and do nothing else
            elsif opts[:search] ## local search when we only know the generic name
                            ## from the db
                find_itself name
            else
                @name = remove_ext(name)
                @path = ::File.dirname(name)
                @cname = ::File::basename(name)
                @full_path = @path + "/" + @cname
            end
            @downloaded = false
        end
        def downloaded?
            @downloaded
        end
        # custom writer so it updates relative attributes also
        def path= param 
            @path = param
            @full_path = @path + "/" + @cname
        end
        def path
            @path
        end

        def ==(other)
            return true if @name == other.name
            return false
        end
        alias :eql? :== 

        def hash
            @name.hash
        end

        # return true if the file is a zipped file
        def zip?
            find_itself unless @full_path
            if @full_path.end_with?(".GZ") || @full_path.end_with?(".gz")
                return true
            end
            false
        end

        ## compress the file and change inner variables
        def zip!
            find_itself unless @full_path
            cmd = "gzip -f \"#{@full_path}\""
            out,err,s = Open3.capture3(cmd)
            unless s.success?
                Logger.<<(__FILE__,"ERROR","CDR::File could not zip itself (#{@cname}). #{err}.Abort.");
                return false
            end
            find_itself @full_path    
            true
        end

        def to_s
            "CDR::File #{@name} (exists = #{::File.exists?(@path + "/" +@cname)}) Compressed : #{zip?}"
        end
        def unzip!
            cmd = "gunzip -f \"#{@full_path}\""
            out,err,s = Open3.capture3(cmd)
            unless s.success?
                Logger.<<(__FILE__,"ERROR","CDR::File uncompress error ...#{err}")
                return false
            end
            @cname = @name
            @full_path = @path + "/" + @cname
            return true
        end

        def delete
            cmd = "rm #{@full_path}"
            unless system(cmd)
                Logger.<<(__FILE__,"ERROR","CDR::File could not be deleted #{@full_path}")
            end
        end

        private

        # will try to find the value of cname
        # searching in the directory
        def find_itself name_
            #binding.pry
            @full_path = nil
            path = ::File::dirname(name_)
            name = ::File::basename(name_)
            f = path + "/" + name
            ["",".gz",".GZ"].each do |ext|
                f_ = f + ext
                if ::File.exists? f_
                    @full_path = f_
                    break
                end
            end
            str = "CDR:File cannot find it self ... #{path}/#{name}"
            (Logger.<<(__FILE__,"ERROR",str);raise str;) unless @full_path
            @cname = ::File.basename(@full_path)
            @name = remove_ext name
            @path = path
        end
        # remove the extension pf the name
        def remove_ext name
            ::File.basename(name.gsub(/(\.GZ|\.gz)/,""))
        end

    end

    ## create a file used by create_tables
    #from a decoded output with the format
    #field:MysqlType
    #used to create multiple table easily directly from
    #the output of the decoder
    #snend this file to create_table after
    def self.dump_table_file json,file_name
        open(file_name,"w") do |file|
            json.each do |record|
                values = record[:values].first
                record[:fields].each_with_index do |field,index|
                    str = field + ":"
                    str << "CHAR(#{values[index].length}) DEFAULT ''"
                    file.puts str
                end
            end
        end
    end
end