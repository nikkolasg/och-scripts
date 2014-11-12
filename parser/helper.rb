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
        when :backlog
            # return the rest because so many possiblites
            return [:backlog,argv]
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
        when :flow
            return unless hash[:flow]
            hash[:flow].call(sub) 
        when :cdr
            return unless hash[:cdr]
            hash[:cdr].call(sub) 
        when :records
            return unless hash[:records]
            hash[:records].call(sub) 
        when :monitor 
            return unless hash[:monitor]
            hash[:monitor].call(sub)
        when :backlog
            hash[:backlog].call(sub) 
        end
    end

end
