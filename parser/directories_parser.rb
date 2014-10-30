require_relative '../logger'

class  DirectoriesParser

    @actions = [:setup,:reset]
    class << self
        attr_accessor :actions
        include './parser/helper'
    end

    def parse argv,opts = {}
        (Logger.<<(__FILE__,"ERROR","No action given to setup utility. Abort."); abort;) unless argv.size > 0
        action = argv.shift.downcase.to_sym

        (Logger.<<(__FILE__,"ERROR","Setup action unknown. Abort.");abort;) unless DirectoriesParser.actions.include? action

        DirectoriesParser.send(action,argv)
    end 

    def self.setup argv,opts
        require './checkDir'
    end

    def self.reset argv,opts
        Logger.<<(__FILE__,"WARNING","No Reset action for Directories yet.Abort.")
        abort
    end
    # to integrate
    def self.backup argv,opts
        str = "Operation reset backup folders on"
        type,sub = Parser::parse_subject argv
        fetcher = Fetchers::create(:LOCAL,{})
        base = App.directories.backup(opts[:dir])
        case type
        when :all
            App.flows.each do |flow|
                flow.switches.each do |switch|
                    path = base + "/" + switch
                    fetcher.delete_files_from(path)
                end
            end
        when :flow
            sub.switches.each do |switch|
                path = base + "/" + switch
                fetcher.delete_files_from(path)
            end
        end
    end

end
