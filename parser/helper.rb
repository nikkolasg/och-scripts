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
            flow_name = argv.shift
            flow = Conf::flow(flow_name)
            (Logger.<<(__FILE__,"ERROR","Operation: flow unknown(#{flow_name}). Abort."); abort;) unless flow
            return [sub, flow]
        when :source,:files,:records
            (Logger.<<(__FILE__,"ERROR","Operation: No source given! Abort."); abort;) unless argv.size > 0
            source = argv.shift
            s = nil
            Conf::flows.each do |flow|
                s = flow.sources(source)
                break if s
            end
            (Logger.<<(__FILE__,"ERROR","Operation: No source found for this name #{source} ... Abort."); abort;) unless s
            return [sub,s]
        when :monitor
            (Logger.<<(__FILE__,"ERROR","Operation: no monitor given ! Abort."); abort;) unless argv.size > 0
            mon = argv.shift.upcase
            (Logger.<<(__FILE__,"ERROR","Operation: monitor does not exists.Abort.");abort;) unless (mon = Conf::monitors(mon))
            return [:monitor,mon]
        when :source
            (Logger.<<(__FILE__,"ERROR","Operation: source not specified ! Abort."); abort) unless argv.size > 0
            name = argv.shift
            source = Conf::flows.inject([]) do |col,flow|
                s = flow.sources(name)
                col << s if s
            end.first
            (Logger.<<(__FILE__,"ERROR","Operation: source does not exists! Abort.");abort) unless source
            return [:source,source]
        when :backlog
            # return the rest because so many possiblites
            return [:backlog,argv]
        when :backup ## the files that are saved
             (Logger.<<(__FILE__,"ERROR","Operation: source not specified ! Abort."); abort) unless argv.size > 0
             return [:backup,argv]
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
            Conf::flows.each do |flow|
                hash[:flow].call(flow)
            end
        else
            hash[type].call(sub) if hash[type]
        end
    end

end