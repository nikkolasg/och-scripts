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
require_relative '../logger'
module Parser
    class  DirectoriesParser
        KEYWORDS = [:folder]
        @actions = [:setup,:reset,:delete,:restore]
        require_relative 'helper'
        class << self
            attr_accessor :actions
            include Parser
        end

        def self.parse argv,opts = {}
            (Logger.<<(__FILE__,"ERROR","No action given to setup utility. Abort."); abort;) unless argv.size > 0
            action = argv.shift.downcase.to_sym

            (Logger.<<(__FILE__,"ERROR","Setup action unknown. Abort.");abort;) unless DirectoriesParser.actions.include? action

            DirectoriesParser.send(action,argv,opts)
        end 

        def self.setup argv,opts
            dirs = Conf::directories
            meth = method(:exists_or_create)
            exists_or_create dirs.data
            exists_or_create "locks" ## locks folder containing locks file
            hash = {}
            hash[:source] = Proc.new do |source|
                [:tmp,:store,:backup].each do |f|
                    d = dirs.send(f)
                    meth.call(d)
                    dd = File::join(d,source.name.to_s)
                    meth.call dd
                    source.folders.each do |folder|
                        meth.call File::join(dd,folder)
                    end
                end 
                log  = Conf::logging
                meth.call log.log_dir
            end
            hash[:flow] = Proc.new do |flow|
                flow.sources.each do |s|
                    hash[:source].call(s)
                end
            end
            take_actions argv,hash
        end

        require_relative '../config/file_manager'
        ## Restore the backup files for this source
        def self.restore argv,opts
            h = {}
            h[:source] = Proc.new do |source|
                m = Conf::LocalFileManager.new
                m.restore_source_files source
            end
            
            h[:flow] = Proc.new do |flow|
                flow.sources.each do |s|
                    h[:source].call(s)
                end
            end
            take_actions argv,h
        end

        ## delete the server files + backup files
        def self.reset argv,opts
            h = {}
            h[:source] = Proc.new do |source|
                m = Conf::LocalFileManager.new
                m.delete_source_files  source
                Logger.<<(__FILE__,"INFO","Deleted files  from #{source.name}")
            end
            h[:flow] = Proc.new do |flow|
                flow.sources.each do |s|
                    h[:source].call(s)
                end
            end
            take_actions argv,h
        end

        ## Delete the all folders for a source
        def self.delete argv,opts
            h = {}
            sourceProc = Proc.new do |source|
                fm = Conf::LocalFileManager.new
                [:tmp,:store,:backup].each do |d|
                    fold = Conf::directories.send(d) 
                    source.folders.each do |folder|
                        url = File.join(fold,source.name.to_s,folder)
                        fm.delete_dir(url)
                        Logger.<<(__FILE__,"INFO","Deleted all files in #{folder} for #{source.name}...")
                    end
                end
            end
            h[:flow] = Proc.new do |flow|
                flow.sources.each do |source|
                    sourceProc.call(source)
                end
            end
            h[:source] = sourceProc
            take_actions argv,h
        end
        private 

        def self.exists_or_create dir
            str =  "Check directory #{dir} ... "
            if !Dir.exists? dir
                Dir.mkdir dir
                str <<  "created.\n"
            else
                str << "checked.\n"
            end
            Logger.<<(__FILE__,"INFO",str)
        end


    end
end