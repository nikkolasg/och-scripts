require_relative '../logger'
module Parser
    class  DirectoriesParser
        KEYWORDS = [:directories]
        @actions = [:setup,:reset,:delete]
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
            exists_or_create dirs.data
            flows = Conf::flows
            [:tmp,:store,:backup].each do |f|
                d = dirs.send(f)
                exists_or_create d
                Conf::flows.each do |flow|
                    flow.sources.each do |source|
                        dd = d + "/" + source.name.to_s
                        exists_or_create dd
                        source.folders.each do |folder|
                            exists_or_create dd + "/" + folder
                        end
                    end
                end
            end 
            log  = Conf::logging
            exists_or_create log.log_dir
        end

        def self.reset argv,opts
            require_relative '../config/file_manager'

            backup = Proc.new do |argv_|
                h = {}
                h[:source] = Proc.new do |source|
                    m = Conf::LocalFileManager.new
                    source.folders.each do |folder|
                        m.delete_files_from File.join(Conf::directories.backup,source.name.to_s,folder)
                        Logger.<<(__FILE__,"INFO","Deleted backup #{folder} from #{source.name}")
                    end
                end
                take_actions argv_,h
            end
            take_actions argv,{backup: backup}
        end

        def self.delete argv,opts
            h = {}
            sourceProc = Proc.new do |source|
                fm = Conf::LocalFileManager.new
                [:tmp,:store,:backup].each do |d|
                   fold = Conf::directories.send(d) 
                   source.folders.each do |folder|
                       url = File.join(fold,source.name,folder)
                       fm.delete_dir(dir)
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
