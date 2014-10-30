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

        when :flow
            (Logger.<<(__FILE__,"ERROR","Operation: no flow given ! Abort."); abort;) unless argv.size > 0
            flow = argv.shift.upcase.to_sym
            flow = App.flow(flow)
            (Logger.<<(__FILE__,"ERROR","Operation: flow unknown. Abort."); abort;) unless flow
            return [:flow, flow]
        when :monitor
            (Logger.<<(__FILE__,"ERROR","Operation: no monitor given ! Abort."); abort;) unless argv.size > 0
            mon = argv.shift.upcase
            (Logger.<<(__FILE__,"ERROR","Operation: monitor does not exists.Abort.");abort;) unless App.monitors.include? mon
            mon = App.monitors mon
            return [:monitor,mon]
        when :backlog
            if argv.size > 0 ## folder specified
                folder =  argv.shift 
                folder = folder[0..-1] if folder[-1] == "/"

                (Logger.<<(__FILE__,"ERROR","Operation: folder does not exists ! Abort."); abort;) unless Dir.exists? folder

                files = Dir.glob(folder + "/*")
                return [:backlog,files]
            elsif (!$stdin.tty? && files = $stdin.read)
                files = files.split("\n")
                return [:backlog,files]
            else
                Logger.<<(__FILE__,"ERROR","Operation: no files or folder specified .... Abort.")
                abort
            end
        end
    end

    def take_actions argv,action_flow,action_backlog,action_monitor = nil
        type,sub = parse_subject argv

        case type
        when :all
            App.flows.each do |flow|
                action_flow.call(flow)
            end
        when :flow
            action_flow.call(sub)
        when :backlog
            action_backlog.call(sub)
        when :monitor 
            action_monitor.call(sub) if action_monitor
        end
    end

end
