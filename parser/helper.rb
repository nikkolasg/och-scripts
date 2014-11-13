## Module that includes some facility to parse
#
module Parser
    # parse the subject
    # and verify its validity (i.e. verify if a folder exists,
    # if a flow exists etc)
    # return [ typeOfSubject, Subject ]
    # typeOfSubject => :all,:flow,:backlog etc
    def parse_subject argv
        (Logger.<<(__FILE__,"ERROR","Operation: No subject given. Abort");abort;) unless argv.size > 0
        sub = argv.shift.downcase.to_sym
        case sub

        when :all
            return [:all,nil]

        when :flow,:cdr,:records
            (Logger.<<(__FILE__,"ERROR","Operation: no flow given ! Abort."); abort;) unless argv.size > 0
            flow_name = argv.shift
            flow = App.flow(flow_name)
            (Logger.<<(__FILE__,"ERROR","Operation: flow unknown(#{flow_name}). Abort."); abort;) unless flow
            return [sub, flow]
        when :monitor
            (Logger.<<(__FILE__,"ERROR","Operation: no monitor given ! Abort."); abort;) unless argv.size > 0
            mon = argv.shift.upcase
            (Logger.<<(__FILE__,"ERROR","Operation: monitor does not exists.Abort.");abort;) unless (mon = App.monitors(mon))
            return [:monitor,mon]
        when :source
            (Logger.<<(__FILE__,"ERROR","Operation: source not specified ! Abort."); abort) unless argv.size > 0
            name = argv.shift
            source = App.flows.inject([]) do |col,flow|
                s = flow.sources(name)
                col << s if s
            end.first
            (Logger.<<(__FILE__,"ERROR","Operation: source does not exists! Abort.");abort) unless source
            return [:source,source]
        when :backlog
            # return the rest because so many possiblites
            return [:backlog,argv]
        else
            Logger.<<(__FILE__,"ERROR","Subject parsing error : #{argv.inspect}")
            raise "Parsing error"
        end
    end

    # utility method to get DRY code
    def take_actions (argv,hash)
        type,sub = parse_subject argv
    
        case type
        when :all
            return unless hash[:flow]
            App.flows.each do |flow|
                hash[:flow].call(flow)
            end
        else
            hash[type].call(sub) if hash[type]
        end
    end

end
