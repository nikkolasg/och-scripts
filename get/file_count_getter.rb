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
module Getter
    require_relative '../ruby_util'
    require_relative '../logger'
    require_relative '../logger'
    
    ## Class that will only record the number of files for the source
    # it will NOT download them
    # The schema will store the info as usual
    class FileCountGetter
        include Getter

        def initialize(source,infos)
            @current_source = source
            @opts = infos
            @take = infos[:take]
            @files = {}
            @current_source.set_options(@opts)
        end

        def get
            Logger.<<(__FILE__,"INFO","Starting GET opertions in #{self.class.name}..")
           get_remote_files 
           count = filter 
           if count == 0
               Logger.<<(__FILE__,"INFO","Nothing new for #{@current_source.name.to_s} :|")
               return
           end
           Logger.<<(__FILE__,"INFO","Filtering on remote files done ... will store #{count} files entries")
           @current_source.schema.insert_files @files
           Logger.<<(__FILE__,"INFO","Files inserted in the system !")
           Logger.<<(__FILE__,"INFO","GET operation finished.")
        end
    end

end