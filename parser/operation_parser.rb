##
#Class that will handle the parsing 
#of all common operation 
#i.e get, insert and process

class OperationParser
    require './parser/helper'
    @usage = "Type can be all, flow , or files. If all, all flows will be operated on.
        With flow, you can specify which flow you wanna get in subject
        With files, you can either specifiy a folder in subject, or send a list of fill path names of files via stdin"

    @actions = [:get,:insert,:stats ]
    class << self; attr_accessor :actions end
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
        type,sub = Parser::parse_subject argv
        require './get/getter'
        Logger.<<(__FILE__,"INFO","Operation: GET on #{type}")
        case type 
        when :all
            flows = App.flows
            flows.each do |f|
                Getter.create(f.name,opts).get
            end
        when :flow
            Getter.create(sub.name,opts).get
        when :files
            opts[:files] = sub
            Getter.create(:files,opts).get
        end
    end

    def self.get_usage
        "GET type [subject] 
         #{OperationParser.usage}"
    end

    def self.insert argv,opts
        type, sub = Parser::parse_subject argv
        require './insert/inserter'
        Logger.<<(__FILE__,"INFO","Operation: INSERT on #{type}")
     
        case type
            when :all
                flows = App.flows
                flows.each do |f|
                    Inserter.create(f.name,opts).insert
                end
            when :flow
                Inserter.create(sub.name,opts).insert
            when :files
                opts[:files] = sub
                Inserter.create(:files,opts).insert
            end
    end

    def self.insert_usage
        "INSERT type [subject]
        #{OperationParser.usage}"
    end

    def self.stats argv,opts
        require './stats/process'
        type,sub = Parser::parse_subject argv
        Logger.<<(__FILE__,"INFO","Operation: STATS on #{type}")
        case type
        when :all
            flows = App.flows
            flows.each do |f|
                Stats::create(f.name,opts).compute
            end
        when :flow
            Stats::create(sub.name,opts).compute
        end
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
