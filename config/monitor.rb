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
module  Conf
    require_relative 'stats'
    class Monitor
        include Conf
        require_relative '../database/monitor_schema'
        require_relative '../database/monitor2_schema'
        [:time_interval,:filters,:flow,:table_stats,:time_field].each do |f|
            Monitor.class_eval(Conf.define_accessor(f))
        end

        def initialize flow,name
            @flow = flow
            @sources = []
            self.send(:name,name)
            @schema = ::Database::Schema::Monitor::GenericSchema.new self
        end       

        def schema klass_name = nil
            if klass_name
                @schema = ::Database::Schema::Monitor::create(klass_name,self)
            else
                return @schema
            end
        end

        def name (param = nil)
            if param
                @name = param
            else
                return @name
            end
        end

        # return the folders where this monitor looks up
        def folders
            @folders ||= self.send(:sources).map do |s|
                s.switches
            end.flatten(1).uniq
        end

        # return the sources associated with this monitor
        def sources *names
            ## reader
            return @sources if names.size == 0
            ## affectation
            names.each do |sname|
                s = @flow.sources(sname)
                @sources << s if s
                raise "Unknown source #{sname} in monitor #{name}!" unless s
            end
        end
        alias :source :sources
      
       ## you can specify a custom attribute to select for the time here 
       def time_field field = nil
          @time_field = field if field
          @time_field
       end 
               
        def reset_stats
            @stats.reset
        end

        def to_s
            return @name
        end

        
    end



end