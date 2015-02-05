## DSL definition for the configuration file
# module Conf contains all the definitions
# and sub classes for each parts of the config
# like Directories, Logging, Flow, Source etc etc
# after writing the config, simply do
# Conf.app_name
# App.flow(:MSS)
# App.flow(:MSS).table_cdr
# App.flow(:MSS).sources(:input).each do |source|
#   source.host
#end
#etc
require_relative '../ruby_util'
module Conf

    module_function 

    require_relative 'filter'

    def define_accessor f
        "def #{f}(param=nil); param.nil? ? @#{f} : (@#{f} = param);end"
    end
    def define_inout_reader f
        "def #{f} (param=nil);
            if param
                if param == :input
                    return @#{f}
                elsif param == :output
                    return @#{f} + Conf.database.output_suffix
                end
            end
            @#{f}
         end"
    end

    def logging &block
        if block_given?
            @logging = Logging.new
            @logging.instance_eval(&block)
        else
            @logging
        end
    end

    def database &block
        if block_given?
            @database = Database.new
            @database.instance_eval(&block)
        else
            @database
        end
    end

    def directories &block
        if block_given?
            @directories = Directories.new
            @directories.instance_eval(&block)
        else 
            @directories
        end
    end

    def app name , &block
        instance_eval(define_accessor("app"))
        @app = App.new(name)
        @app.instance_eval(&block)
    end


    def flow name_, &block
        require_relative 'flow'
        name = name_.upcase.to_sym
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

    def flows
        @flows
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
            h = Host.new
            h.name name
            h.instance_eval(&block)
            @hosts << h
        else
            return @hosts.select { |h|  h.name == name }.first
        end
    end

    def make &block
        str = "Parsing config file ..."
        begin
            instance_eval(&block) 
        rescue => e
            str << "Error ! #{e.message}\n#{e.backtrace.join("\n")}"
            $stderr.puts str #Logger. is not ready yet 'cause uses the Conf.config
            abort
        end
        str << " Ok :) "
        require_relative '../logger'
        Logger.<<(__FILE__,"INFO","#{@app.name} (v. #{@app.version}) starting")
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

end
#########################################
#subclasses for each categories of the config
#Flow, Source, Log, Directories, Database ...
#########################################

module Conf

    class App  

        @@fields = [ :name,:version, :author]
        @@fields.each do |f|
            App.class_eval("def #{f}(param=nil); param.nil? ? @#{f} : (@#{f} = param);end")
        end

        def initialize name
            @name = name
            @contributers = []
        end

        def contributers *args
            if args.size > 0
                @contributers += args
            else
                @contributers
            end
        end
    end


    class Host
        include Conf
        @@fields = [:name,:address,:login,:password,:protocol,:database]
        @@fields.each do |f|
            Host.class_eval(Conf.define_accessor(f))
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
        include Conf
        @@fields = [:host,:name,:login,:password,:timestamp,:data_directory,:index_directory]
        @@fields.each do |f|
            Database.class_eval(Conf.define_accessor(f))
        end
        def initialize
            @timestamp = "timest"
        end


    end

    class Logging
        include Conf
        @@fields = [:log_dir,:stdout,:level_log,:level_email,:level_sms ]
        @@fields.each do |f|
            Logging.class_eval(Conf.define_accessor(f))
        end

        ## since we expect a hash, ruby automatically 
        #merge the different arguments together
        def levels (args)
            if !instance_variable_defined?(:@log_level)
                Logging.class_eval(Conf.define_accessor("log_level"))
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
        include Conf
        [:app,:output_suffix,:database_dump].each do |f|
            Directories.class_eval(Conf.define_accessor(f))
        end
        # same but more meta programming style ;)
        [:store,:tmp,:backup].each do |f|
            define_method f do | param = nil |
                var = "@#{f}".to_sym
                if param 
                    instance_variable_set(var,param)
            else
                return @data + "/" + instance_variable_get(var) 
            end
            end
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
    end
end


