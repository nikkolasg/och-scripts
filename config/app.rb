## DSL definition for the configuration file
# module App contains all the definitions
# and sub classes for each parts of the config
# like Directories, Logging, Flow, Source etc etc
# after writing the config, simply do
# App.app_name
# App.flow(:MSS)
# App.flow(:MSS).table_cdr
# App.flow(:MSS).sources(:input).each do |source|
#   source.host
#end
#etc
module Printer
    #def to_s
        #instance_variables.each do |var|
            #puts "#{var}  => " + instance_variable_get("#{var}").to_s
        #end
    #end
end
module App

    include Printer         
    ## MODULE    
    extend self
    @@fields = [ :app_name,:app_version]
    @@fields.each do |f|
        instance_eval("def #{f}(param=nil); param.nil? ? @#{f} : (@#{f} = param);end")
    end

    def define_accessor f
        "def #{f}(param=nil); param.nil? ? @#{f} : (@#{f} = param);end"
    end

    def flow name, &block
        name = name.upcase.to_sym
        # the "search flow function" is called here
        return find_flow(name) if !block_given?

        # here we define the flow !
        if !instance_variable_defined?(:@flows)
            instance_eval(define_accessor("flows"))
            @flows = []
        end
        flow = Flow.new name
        @flows << flow
        flow.instance_eval(&block)
    end 

    def logging &block
        attr_accessor :logging
        @logging = Logging.new
        @logging.instance_eval(&block)
    end

    def database &block
        attr_accessor :database
        @database = Database.new
        @database.instance_eval(&block)
    end

    def directories &block
        attr_accessor :directories
        @directories = Directories.new
        @directories.instance_eval(&block)
    end

    def config &block
        str = "Parsing config file ..."
        begin
            instance_eval(&block) 
        rescue => e
            str << "Error ! #{e.message}"
            $stderr.puts str
        end
        str << " Ok :) "
        Logger.<<(__FILE__,"INFO","#{App.app_name} (v. #{App.app_version}) starting")
        Logger.<<(__FILE__,"INFO",str)
    end

    # find a flow by its name in the array of flow
    def find_flow name
        @flows.select { |f| f.name == name }.first
    end
    def summary
        puts "App Config Description : "
        to_s
    end



    #########################################
    #subclasses for each categories of the config
    #Flow, Source, Log, Directories, Database ...
    #########################################

    class Flow
        include Printer
        [ :name,:out_suffix,:decoder,:test_file].each do |f|
            Flow.class_eval(App.define_accessor(f))
        end
        ## create custom accessor when the direction is supplied
        # u can call it like this
        # flow.table_cdr(:input) ==> CDR_MSS
        # flow.table_cdr         ==> CDR_MSS (input is default)
        # flow.table_cdr(:output)==> CDR_MSS_OUT
        # flow.table_cdr "CDR_MSS" ==> affectation !
        [ :table_cdr,:table_records,:table_stats ].each do |f|
            str = "def #{f}(param=nil)
                    if param
                        if :input == param
                            return @#{f}
                        elsif :output == param
                            return @#{f} + @out_suffix
                        else 
                            @#{f} = param
                        end
                    end
                    @#{f} 
               end"
            Flow.class_eval(str)
        end 
        ## Create custom access
        #   when specified cdr_fields_file , it will
        #   trigger the reading of the file 
        #   and put the fields into cdr_fields !!
        [ :cdr_fields,:records_fields,:records_fields].each do |f|
            str = "def #{f.to_s + "_file"}(param=nil)
                        if param
                            @#{f.to_s + "_file"} = param
                            @#{f} = parse_fields_file(param)
                        else
                            @#{f.to_s + "_file"}
                        end
                   end"
           ## define the fields attributes
           Flow.class_eval(App.define_accessor(f))
           Flow.class_eval(str)

        end



        def initialize name
            @name = name
            @sources = []
            @monitors = []
        end
        def source  name, &block
            newSource = Source.new
            newSource.name (name.downcase.to_sym)
            @sources << newSource
            newSource.instance_eval(&block)
        end

        def sources search = nil
            if search ## if we want specific source
                if [:input,:output].include?( search)
                    # we want sources from a given direction
                    return @sources.select { |s| s.direction == search }
                else
                    # we want source from a given name
                    return @sources.select { |s| s.name == search }.first
                end
            else
                @sources
            end
        end

        ## return all the switches for this flow
        #  
        def switches
            @sources.map { |s| s.switches }.flatten(1)
        end
        
        def records_allowed *args
            if args.size == 0 ## no args, ==> accesssor
                @records_allowed
            else ## affectation
                @records_allowed = args.join(',')
            end
        end

        def monitor name, &block
            m = Monitor.new
            m.name name
            @monitors << m
            m.instance_eval(&block)
        end
        # accesor of monitors by name or all the monitors
        def monitors name = nil
            if name
                @monitors.select { |m| m.name == name }
            else
                @monitors
            end
        end
           
        private
        # read the fields, and create custom accessor for it
        # file must be in format
        # column_name:SQL TYPE
        # output format is a Hash
        # key => column_name
        # value => sql type
        def parse_fields_file file
            hash = {}
            File.read(file).split("\n").each do |line|
                field,sql = line.split ':'
                hash[field] = sql
            end
            hash
        end
    end

    class Source
        include Printer
        @@fields =[ :name,:direction, :host, :base_dir , :login,:password, :regexp ]
        @@fields.each do |f|
            Source.class_eval(App.define_accessor(f))
        end

        def switch(*args)
            if !instance_variable_defined?(:@switches)
                Source.class_eval(App.define_accessor("switches"))
                @switches = []
            end
            @switches = @switches + args
        end
        def protocol(param = nil)
            if param 
                @protocol = param.upcase.to_sym
            else
                @protocol
            end
        end
    end

    class Monitor
        
       [:input,:output].each do |f|
           str = "def #{f}(*param)
                    if param.size > 0
                        @#{f} = @#{f} + param.map {|p| p.downcase.to_sym }
                    else
                        @#{f}
                    end
                  end"
          Monitor.class_eval(str)
       end

       [:time_interval,:aggregate_by,:name].each do |f|
           Monitor.class_eval(App.define_accessor(f))
       end

          
        def initialize
            @input = []
            @output = []
            self.aggregate_by :time # default
        end       

    end

    class Database 
        include Printer
        @@fields = [:host,:name,:login,:password,:timestamp]
        @@fields.each do |f|
            Database.class_eval(App.define_accessor(f))
        end

    end

    class Logging
        include Printer
        @@fields = [:log_dir,:stdout,:level_log,:level_email,:level_sms ]
        @@fields.each do |f|
            Logging.class_eval(App.define_accessor(f))
        end

        def levels (args)

            if !instance_variable_defined?(:@log_level)
                Logging.class_eval(App.define_accessor("log_level"))
                @log_level = {}
            end
            args.each do |k,v|
                @log_level[k] = v
            end
        end
    end

    ## old style made class
    #  to directly transform relative path
    #  into full path
    class Directories
        include Printer
        [:app,:out_suffix,:database_dump].each do |f|
            Directories.class_eval(App.define_accessor(f))
        end

        def data(param=nil)
            if !param
                @data
            else ## affectation
                @data = @app + "/" + param
            end
        end
        ## accesor and affectation at the same time
        #if specified a direction => add suffix
        #if specified other => affectation
        #else return value
        def tmp(param=nil)
            ret = dir @tmp,param
            ret.nil? ? (@tmp = @data + "/" + param) : ret
        end

        def store(param=nil)
            ret = dir @store,param
            ret.nil? ? (@store = @data+"/"+param) : ret
        end

        def backup(param=nil)
            ret = dir @backup,param
            ret.nil? ? (@backup = @data + "/" + param) : ret
        end

        private 
        # if we wants access, return the right value
        # if we want affectation, return nil
        # so calling method know what to do
        def dir field,param
            if param == :output || param == :out
                field + @out_suffix
            elsif [:input,:in].include?(param) || !param
                field
            else
                nil
            end
        end
    end
end


