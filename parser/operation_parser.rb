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
##
#Class that will handle the parsing 
#of all common operation 
#i.e get, insert and process
module Parser
class OperationParser
    @usage = "Type can be all, flow , or backlog. If all, all flows will be operated on.
        With flow, you can specify which flow you wanna get in subject
        With backlog, you can either specifiy a folder in subject, or send a list of fill path names of files via stdin"
    ## RUN action is the three actions combined (get/insert/process) in a row
    @actions = [:get,:insert,:process,:run ]
    KEYWORDS = @actions
    require_relative 'helper'
    
    class << self 
        attr_accessor :actions 
        include Parser
    end
    ## special shortcut for operation since
    # theses will be the main action taken by monitor
    # so no need to specify "monitor operation get|insert" etc
    # so there's no check on "operation" key word, and we 
    # directly take the action from argv (from the opts since it has been removed)
    def self.parse argv, opts = {}

        action = opts[:argv].shift.downcase.to_sym
        (Logger.<<(__FILE__,"ERROR","Operation: action unknown (#{action}). Abort"); abort;) unless OperationParser.actions.include? action

        OperationParser.send(action,argv, opts)
    end

    def self.run argv,opts
        OperationParser.get argv.dup,opts
        OperationParser.insert argv.dup,opts
        OperationParser.process argv.dup,opts
    end
    def self.get argv,opts
        require_relative '../get/getter'
        ah = {}
        ah[:flow] = Proc.new { |flow|
                               flow.sources.each do |source|
                                source.get(opts); 
                               end
                            }
        ah[:source] = Proc.new { |source| 
                                 source.get(opts)
                                }
        take_actions(argv,ah)
    end

    def self.get_usage
        "GET type [subject] 
        #{OperationParser.usage}"
    end

    def self.insert argv,opts
        require_relative '../insert/inserter'
        h = {}
        h[:flow] = Proc.new { |flow| 
            flow.sources.each do |source|
                source.insert opts
            end
        }
        h[:source] = Proc.new { |source| 
            source.insert opts
        }
        take_actions argv,h
    end

    def self.insert_usage
        "INSERT type [subject]
        #{OperationParser.usage}"
    end

    def self.process argv,opts
        require_relative '../process/processer'
        flow_action = Proc.new { |flow| 
            opts[:flow] = flow
            Stats::create(:generic,opts).compute }
        monitor_action = Proc.new do |monitor| 
            opts[:monitor] = monitor
            opts[:flow] = monitor.flow
            Stats::create(:generic,opts ).compute
        end
        source_ = Proc.new do |source|
            source.flow.monitors.each do |mon|
                next unless  mon.sources.include?(source)
                opts[:source] = source
                opts[:flow] = source.flow
                Stats::create(:generic,opts).compute 
            end
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

        take_actions argv,{ flow: flow_action, source: source_,monitor: monitor_action,backlog: backlog_action }
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
end