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
    class DatabaseParser
        require_relative '../database'
        KEYWORDS = [:database]
        @actions = [:setup,:reset,:delete,:rotate,:dump,:rename]

        require_relative  'helper'         
        class << self
            attr_accessor :actions
            include Parser
        end

        def self.parse(argv,opts = {})
            (Logger.<<(__FILE__,"ERROR","No action given to database utility. Abort."); abort;) unless argv.size > 0
            action = argv.shift.downcase.to_sym

            (Logger.<<(__FILE__,"ERROR","Database action unknown. Abort.");abort;) unless self.actions.include? action

            DatabaseParser.send(action,argv,opts)
        end

        # create the tables used by the monitoring tool
        def self.setup argv, opts 
            ah = {}
            ah[:flow] = Proc.new do |flow|
                flow.sources.each do |source|
                    source.schema.set_options opts
                    source.schema.create :files
                    source.schema.create :records
                end
                flow.monitors.each do  |mon|
                    mon.schema.create :stats
                    mon.schema.create :files
                end
            end
            ah[:source] = Proc.new do |source|
                source.schema.set_options opts
                source.schema.create :files
                source.schema.create :records
            end
            ah[:files] = Proc.new do |source|
                source.schema.set_options opts
                source.schema.create :files
            end
            ah[:records]= Proc.new do |source|
                source.schema.set_options opts
                source.schema.create :records
            end
            ah[:monitor] = Proc.new do |mon|
                mon.schema.create :stats
                mon.schema.create :files
            end
            take_actions argv,ah
        end

        # flush the tables 
        def self.reset argv, opts
            ah = {}

            fm = Conf::LocalFileManager.new
            ah[:flow] = Proc.new do |flow|
                flow.sources.each do |source|
                    source.set_options opts
                    source.schema.reset :files
                    source.schema.reset :records
                    fm.delete_source_files source
                end
                flow.monitors.each do |mon|
                    mon.schema.reset :stats
                    mon.schema.reset :files
                end
            end
            ah[:files] = Proc.new do |source|
                source.set_options opts
                source.schema.reset :files
                fm.delete_source_files source
            end
            ah[:records]= Proc.new do |source|
                source.set_options opts
                source.schema.reset :records
                fm.restore_source_files source
                source.schema.reset_files 
            end

            ah[:source] = Proc.new do |source|
                source.set_options opts
                source.schema.reset :files
                source.schema.reset :records
                fm.delete_source_files source
            end
            ah[:monitor]= Proc.new do |mon|
                mon.schema.reset :stats
                mon.schema.reset :files
            end
            take_actions argv,ah
        end

        # delete the tables
        def self.delete argv, opts
            print = Proc.new do |subject,tables|
                Logger.<<(__FILE__,"INFO","Deleted operation terminated for #{subject.name} => #{tables.join(',')}")
            end
            ahash = {}
            fm = Conf::LocalFileManager.new
            ahash[:flow] = Proc.new do |flow|
                flow.sources.each do |source|
                    source.set_options opts
                    source.schema.delete :files
                    source.schema.delete :records
                    #fm.delete_source_files source
                end
                flow.monitors.each do |monitor|
                    monitor.schema.delete :stats
                    monitor.schema.delete :files
                end
            end

            ahash[:source] = Proc.new do |source|
                source.set_options opts
                source.schema.delete :files
                source.schema.delete :records
                #fm.delete_source_files source
            end
            ahash[:files] = Proc.new do |source|
                source.set_options opts
                source.schema.delete :files
                #fm.delete_source_files source
            end
            ahash[:records] = Proc.new do |source|
                source.set_options opts
                source.schema.delete :records
                source.schema.reset_files 
                #fm.restore_source_files source
            end

            ahash[:monitor] = Proc.new do |mon|
                mon.schema.delete :stats
                mon.schema.delete :files
            end

            take_actions argv,ahash
        end

        # rotate the tables if necessary  
        def self.rotate argv, opts
            ah = {}

            p = Proc.new do |source|
                Database::TableRotator.source source,opts
            end
            ah[:source] =  p
            ah[:flow] = Proc.new do |flow|
                flow.sources.each do |source|
                    p.call(source)
                end
            end
            take_actions argv,ah
        end

        def self.dump argv,opts
            Logger.<<(__FILE__,"INFO","Database: Dump operation in progress ...")
            ah = {}
            ah[:source] = Proc.new do |source|
                decoder = source.decoder
                decoder.opts.merge! opts 
                decoder.opts[:nb_line] = 10000 unless decoder.opts[:nb_line]
                ::File.delete(decoder.dump_file) if ::File.exists?(decoder.dump_file)
                f = decoder.fields
                Logger.<<(__FILE__,"INFO","Database: Dump have dumped #{f.size} fields into #{decoder.dump_file}")
            end  
            take_actions argv,ah
        end

        def self.rename argv,opts
            h = {}
            h[:source] = Proc.new do |source|
                nname = argv.shift
                fm = Conf::LocalFileManager.new
                ## table 
                source.schema.rename nname 
                source.flow.monitors.each { |m| m.schema.rename_source source,nname if m.sources.include?(source)}
                ## find & renames dirs
                fm.rename_source_files source,nname
                ## rename in config files
                conf_files = fm.ls Conf::directories.app, "config*.rb"
                cmd = "sed -i 's/#{source.name}/#{nname}/' #{conf_files.join(' ')}"
                fm.exec_cmd(cmd)
                Logger.<<(__FILE__,"INFO","Changed name of source into config files ..")
            end     
            take_actions argv,h
        end


    end
end
