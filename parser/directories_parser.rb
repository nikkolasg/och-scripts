require_relative '../logger'

class  DirectoriesParser

    @actions = [:setup,:reset]
    require './parser/helper'
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
        require './checkDir'
    end

    def self.reset argv,opts
        require './file_manager'
        manager = App::LocalFileManager.new
        d = App.directories
        flow = Proc.new do |flow|
            flow.switches.each do |switch|
                Util::starts_for opts[:dir] do |dir|
                    manager.delete_files_from (d.tmp(dir) + "/" + switch) 
                    manager.delete_files_from (d.store(dir) + "/" + switch)
                    manager.delete_files_from (d.backup(dir) + "/" + switch)
                end
                Logger.<<(__FILE__,"INFO","Deleted all files (Cdr + backup)for switch #{switch}")
            end
        end
        cdr = Proc.new do |flow|
            flow.switches.each do |sw|
                Util::starts_for opts[:dir] do |dir|
                   manager.delete_files_from (d.store(dir) + "/" + sw)
                end
                Logger.<<(__FILE__,"INFO","Deleted all CDR files for flow #{flow.name}")
            end
        end
        records = Proc.new do |flow|
            flow.switches.each do |sw|
                Util::starts_for opts[:dir] do |dir|
                    manager.delete_files_From (d.backup(dir) + "/" + sw)
                end
                Logger.<<(__FILE__,"INFO","Deleted all backup CDR files for flow #{flow.name}")
            end
        end
        take_actions argv,{flow: flow,cdr: cdr,records: records}
    end

end
