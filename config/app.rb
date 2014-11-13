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
require './ruby_util'
module App
    ## MODULE    
    extend self
    @@fields = [ :app_name,:app_version,:hosts]
    @@fields.each do |f|
        instance_eval("def #{f}(param=nil); param.nil? ? @#{f} : (@#{f} = param);end")
    end

    def define_accessor f
        "def #{f}(param=nil); param.nil? ? @#{f} : (@#{f} = param);end"
    end
    def define_inout_reader f
        "def #{f} (param=nil);
            if param
                if param == :input
                    return @#{f}
                elsif param == :output
                    return @#{f} + App.database.output_suffix
                end
            end
            @#{f}
         end"
    end

    RubyUtil::require_folder("config")

    def flow name_, &block
        name = name_.upcase.to_sym
        # the "search flow function" is called here
        return find_flow(name) if !block_given?

        # here we define the flow !
        if !instance_variable_defined?(:@flows)
            instance_eval(define_accessor("flows"))
            @flows = []
        end
        flow = App::Flow.new name
        @flows << flow
        flow.instance_eval(&block)
    end 

    def flows
        @flows
    end

    def logging &block
        attr_accessor :logging
        @logging = App::Logging.new
        @logging.instance_eval(&block)
    end

    def database &block
        attr_accessor :database
        @database = App::Database.new
        @database.instance_eval(&block)
    end

    def directories &block
        attr_accessor :directories
        @directories = App::Directories.new
        @directories.instance_eval(&block)
    end

    # find a particular monitor
    def monitors name 
        name = name.downcase.to_sym
        @flows.each do |flow|
            mon = flow.monitors(name)
            return mon if mon
        end
        nil
    end

    def host name, &block
        @hosts ||= []
        if block_given?
            ## affectation
            h = App::Host.new
            h.name name
            h.instance_eval(&block)
            @hosts << h
        else
            return @hosts.select { |h|  h.name == name }.first
        end
    end

    def config &block
        str = "Parsing config file ..."
        begin
            instance_eval(&block) 
        rescue => e
            str << "Error ! #{e.message}\n#{e.backtrace.join("\n")}"
            $stderr.puts str #Logger. is not ready yet 'cause uses the App.config
            abort
        end
        str << " Ok :) "
        require './logger'
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


    class Host
        @@fields = [:name,:address,:login,:password,:protocol]
        @@fields.each do |f|
            Host.class_eval(App.define_accessor(f))
        end
        def protocol(param = nil)
            if param 
                @protocol = param.downcase.to_sym
            else
                @protocol
            end
        end
        def to_s 
            "#{login}@#{name}(#{address})"
        end
    end
    class Database 

        @@fields = [:host,:name,:login,:password,:timestamp,:output_suffix]
        @@fields.each do |f|
            Database.class_eval(App.define_accessor(f))
        end
        def initialize
            @output_suffix = "_OUT"
            @timestamp = "timest"
        end


    end

    class Logging

        @@fields = [:log_dir,:stdout,:level_log,:level_email,:level_sms ]
        @@fields.each do |f|
            Logging.class_eval(App.define_accessor(f))
        end
        
        ## since we expect a hash, ruby automatically 
        #merge the different arguments together
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

        [:app,:output_suffix,:database_dump].each do |f|
            Directories.class_eval(App.define_accessor(f))
        end

        def initialize
            @output_suffix = "_out"
            @database_dump = "dump"
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
                field + output_suffix
            elsif [:input,:in].include?(param) || !param
                field
            else
                nil
            end
        end
    end
end


