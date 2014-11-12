##
#Class that will handle the parsing 
#of all common operation 
#i.e get, insert and process

class OperationParser
    @usage = "Type can be all, flow , or backlog. If all, all flows will be operated on.
        With flow, you can specify which flow you wanna get in subject
        With backlog, you can either specifiy a folder in subject, or send a list of fill path names of files via stdin"

    @actions = [:get,:insert,:process ]
    require './parser/helper'
    
    class << self 
        attr_accessor :actions 
        include Parser
    end
    ## special shortcut for operation since
    # theses will be the main action taken by monitor
    # so no need to specify "monitor operation get|insert" etc
    # so there's no check on "operation" key word, and we 
    # directly take the action from argv
    def self.parse argv, opts = {}
        action = argv.shift.downcase.to_sym
        (Logger.<<(__FILE__,"ERROR","Opertion: action unknown. Abort"); abort;) unless OperationParser.actions.include? action

        OperationParser.send(action,argv, opts)
    end

    def self.get argv,opts
        require './get/getter'
        ah = {}
        ah[:flow] = Proc.new { |flow|  Getter.create(flow.name,opts).get }

        take_actions(argv,ah)
    end

    def self.get_usage
        "GET type [subject] 
        #{OperationParser.usage}"
    end

    def self.insert argv,opts
        require './insert/inserter'
    
        flow_action = Proc.new { |flow| 
            Inserter.create(flow.name,opts).insert }

        take_actions argv,{ flow: flow_action }
    end

    def self.insert_usage
        "INSERT type [subject]
        #{OperationParser.usage}"
    end

    def self.process argv,opts
        require './process/processer'
        flow_action = Proc.new { |flow| 
            opts[:flow] = flow
            Stats::create(:generic,opts).compute }
        monitor_action = Proc.new do |monitor| 
            opts[:monitor] = monitor
            opts[:flow] = monitor.flow
            Stats::create(:generic,opts ).compute
        end
        ## will reprocess the input in "backlog" mode
        backlog_action = Proc.new do |argv_|
            hash = {}
            hash[:monitor] = Proc.new do |monitor|
                opts[:monitor] = monitor
                Stats::create(:backlog,opts).compute
            end
            hash[:flow] = Proc.new do |flow|
                opts[:flow] = flow
                Stats::create(:backlog,opts).compute
            end
            take_actions argv_, hash
        end

        take_actions argv,{ flow: flow_action, monitor: monitor_action,backlog: backlog_action }
    end

    def self.stats_usage
    end

    def self.usage
        str = []
        OperationParser.actions.each do |action|
            n = action + "_usage"
            str << OperationParser.send(n.to_sym) 
        end
        str
    end
end
