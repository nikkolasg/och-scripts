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
    def to_s
        instance_variables.each do |var|
            puts "#{var}  => " + instance_variable_get("#{var}").to_s
        end
    end
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
        instance_eval(&block) 
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
        @@fields = [ :name,:table_cdr,:table_records,:table_stats ]
        @@fields.each do |f|
           Flow.class_eval(App.define_accessor(f))
        end
        def initialize name
            @name = name
        end
        def source  direction, &block
            if !instance_variable_defined?(:@sources)
               #Flow.class_eval(App.define_accessor("sources"))
                @sources = []
            end
            newSource = Source.new
            newSource.direction (direction.downcase.to_sym)
            @sources << newSource
            newSource.instance_eval(&block)
        end

        def sources direction = nil
            if direction
                @sources.select { |s| s.direction == direction }
            else
                @sources
            end
        end

        
    end

    class Source
        include Printer
        @@fields =[ :direction, :protocol, :host, :base_dir , :login,:password ]
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
        [:app,:out_suffix].each do |f|
            Directories.class_eval(App.define_accessor(f))
        end

        def data(param=nil)
            if !param
                @data
            else ## affectation
                @data = @app + "/" + param
            end
        end

        def tmp(param=nil)
            ret = dir @tmp,param
            if ret.nil?
                @tmp = @data + "/" + param
            end
        end

        def store(param=nil)
            ret = dir @store,param
            if ret.nil?
                @store = @data + "/" + param
            end
        end

        def backup(param=nil)
            ret = dir @backup,param
            if ret.nil?
                @backup = @data + "/" + param
            end
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


