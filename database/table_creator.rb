module Datalayer
    ## Utility class to handles (create, delete, reset) all the tables
    #of the databases according to flow, or monitors etc
    class TableCreator
        
        def self.for flow, &block
            db = MysqlDatabase.default
            t = TableCreate.new(flow,db)
            db.connect do 
                t.instance_eval(&block)
            end
        end

        def initialize(flow,db)
            @flow = flow
            @db = db
        end

        def cdr *dirs
            dirs.each do |dir|
                sql = SqlGenerator.for_cdr(@flow.table_cdr(dir))
                @db.query(sql)
            end
        end

        def cdr_union union,*dirs
            dirs.each do |dir|
                sql = SqlGenerator.for_cdr_union(@flow.table_cdr_union(dir),
                                               union: union)
                @db.query(sql)
            end
        end

        def records *dirs
            dirs.each do |dir|
                sql = SqlGenerator.for_records(@flow.table_records(dir),
                                                     @flow.records_fields)
                @db.query(sql)
            end
        end

        def records_union union,*dirs
            dirs.each do |dir|
                sql = SqlGenerator.for_records_union(@flow.table_records_union(dir),
                                                     @flow.records_fields,
                                                     union: union)
                @db.query(sql)
            end
        end

        def monitor subject, &block
            if subject == :all
                @flow.monitors.each do |monitor|
                    @current_monitor = monitor
                    instance_eval(&block) if block_given?
                end
            elsif @flow.monitors(subject.name)
                @current_monitor = subject
                instance_eval(&block) if block_given?
            end
        end

        def table_stats
            sql = SqlGenerator.for_monitor @current_monitor
            @db.query(sql)
        end

        def table_records *dirs
            dirs.each do |dir|
                name = @current_monitor.table_records(dir)
                sql = SqlGenerator.for_monitor_records(name)
                @db.query(sql)
            end
        end

        def table_records_union union,*dirs
            dirs.each do |dir|
                name = @current_monitor.table_records_union(dir)
                sql = SqlGenerator.for_monitor_records_union name,union: union
                @db.query(sql)
            end
        end
    end
   
    ## BIG METAPROGRAMMING SECTION. You've been warned =)
    # Create TableReset & TableDelete at same time,
    # since the functionnal logic is the same
    # but the operationnal (delete/reset) is not

    class TableAction
        ## create a class method so we can call
        #TableReset.for or delete
        #create a INSTANCE of the class and evaluates the block
        def self.for flow, &block
            t = self.new(flow)
            t.instance_eval(&block)
        end
        def initialize flow
            @flow = flow
        end
    end
            
    class TableReset < TableAction
    end
    class TableDelete < TableAction
    end

    def self.construct(klass, action)
        klass.instance_eval do 
            #helper method to get the method name to use
            define_method :method do 
                (action.to_s + "_table").to_sym
            end
                       
            inside_method = Proc.new do |type,dir|
                        # method name
                        m = "table_" + type.to_s 
                        # table name
                        name = @flow.send(m.to_sym,dir)
                        # do the action
                        TableUtil.send(method,name)
            end

            ## for both type define all methods
            ## (monitor cannot be reduced)
            [:cdr,:records].each do |type|
                ## define simple one
                define_method type do |*dirs|
                    dirs.each do |dir|
                        inside_method.call(type,dir)
                    end
                end
                ## define union one
                method_name = type.to_s + "_union"
                define_method method_name.to_sym do |*dirs|
                    dirs.each do |dir|
                        inside_method.call(type,dir)
                    end
                end
            end
            ###################
            ## MONITOR PARTS !!
            ##################
            # define a new BLOCK DSL for monitor part
            define_method :monitor do |&block|
               if subject == :all
                @flow.monitors.each do |monitor|
                    @current_monitor = monitor
                    instance_eval(&block) if block
                end
                elsif @flow.monitors(subject.name)
                    @current_monitor = subject
                    instance_eval(&block) if block
                end
            end
            ## new "actions" to do for monitors,
            #i.e. take the table name from the monitors itself
            #not flow
            inside_method = Proc.new do |type,dir|
                m = "table_" + type.to_s
                name = @current_monitor.send(m.to_sym,dir)
                TableUtil.send(method,name)
            end
            ## define method for the monitors part
            #r/d stats,records,records_union !!
            [:stats,:records,:records_union].each do |type|
                    define_method type do |*dirs|
                        dirs.each do |dir|
                            inside_method.call(type,dir)
                        end
                    end
            end
        end
    end
    self.construct(TableReset,:reset)
    self.construct(TableDelete,:delete)
end
