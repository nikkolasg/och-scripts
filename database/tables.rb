module Database
    ## Utility class to handles (create, delete, reset) all the tables
    #of the databases according to flow, or monitors etc

    class TableCreator

        def self.for flow, &block
            db = Mysql.default
            t = TableCreator.new(flow,db)
            db.connect do 
                t.instance_eval(&block)
            end
        end

        def initialize(flow,db)
            @flow = flow
            @db = db
        end

        def cdr *dirs
            Util::starts_for(dirs) do |dir|
                sql = SqlGenerator.for_cdr(@flow.table_cdr(dir))
                @db.query(sql)
            end
            Logger.<<(__FILE__,"INFO","Created table for #{@flow.name}:CDR")
        end

        def cdr_union union,*dirs
            Util::starts_for(dirs) do |dir|
                sql = SqlGenerator.for_cdr_union(@flow.table_cdr_union(dir),
                                                 union: union)
                @db.query(sql)

            end
            Logger.<<(__FILE__,"INFO","Created table for #{@flow.name}:CDR UNION")
        end

        def records *dirs
            Util::starts_for(dirs) do |dir|
                sql = SqlGenerator.for_records(@flow.table_records(dir),
                                               @flow.records_fields)
                @db.query(sql)

            end
            Logger.<<(__FILE__,"INFO","Created table for #{@flow.name}:Records")
        end

        def records_union union,*dirs
            Util::starts_for(dirs) do |dir|
                sql = SqlGenerator.for_records_union(@flow.table_records_union(dir),
                                                     @flow.records_fields,
                                                     union: union)
                @db.query(sql)

            end
            Logger.<<(__FILE__,"INFO","Created table for #{@flow.name}:Records UNION")
        end

        def monitor subject, &block
            if subject == :all
                @flow.monitors.each do |monitor|
                    @current_monitor = monitor
                    instance_eval(&block) if block_given?
                end
            elsif (mon = @flow.monitors(subject.to_s))
                @current_monitor = mon
                instance_eval(&block) if block_given?
            end
        end

        def table_stats
            sql = SqlGenerator.for_monitor_stats @current_monitor
            @db.query(sql)
            Logger.<<(__FILE__,"INFO","Created table for #{@flow.name}:Monitor #{@current_monitor.name}")

        end

        def table_records *dirs
            Util::starts_for(dirs) do |dir|
                name = @current_monitor.table_records(dir)
                sql = SqlGenerator.for_monitor_records(name)
                @db.query(sql)
            end
            Logger.<<(__FILE__,"INFO","Created table for #{@flow.name}:Monitor_records")
        end

        def table_records_union union,*dirs
            Util::starts_for(dirs) do |dir|
                name = @current_monitor.table_records_union(dir)
                sql = SqlGenerator.for_monitor_records_union name,union: union
                @db.query(sql)
            end
            Logger.<<(__FILE__,"INFO","Created table for #{@flow.name}:Monitor_records UNION")

        end
        def table_records_backlog 
            name = @current_monitor.table_records_backlog
            sql = SqlGenerator.for_monitor_records_backlog name
            @db.query(sql)
            Logger.<<(__FILE__,"INFO","Created table for #{@flow.name}:Monitor records backlog")

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
        attr_accessor :flow
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

            inside_method = Proc.new do |flow,type,dir|
                # method name according to cdr /record
                m = "table_" + type.to_s
                # table name to operate on
                name = flow.send(m.to_sym,dir)
                all_names = TableUtil::search_tables name
                # method name to DO the action (Delete/reset)
                method_name = "#{action}_table"
                all_names.each do |tname|
                    TableUtil.send(method_name,tname)
                end
            end

            ## for both type define all methods
            ## (monitor cannot be reduced)
            [:cdr,:records].each do |type|
                ## define simple one
                define_method type do |*dirs|

                    Util::starts_for(dirs) do |dir|
                        inside_method.call(@flow,type,dir)
                    end

                end
                ## define union one
                method_name = type.to_s + "_union"
                define_method method_name.to_sym do |*dirs|
                    Util::starts_for(dirs) do |dir|
                        inside_method.call(@flow,method_name,dir)
                    end

                end
            end
            ###################
            ## MONITOR PARTS !!
            ##################
            # define a new BLOCK DSL for monitor part
            define_method :monitor do |subject,&block|
                if subject == :all
                    @flow.monitors.each do |monitor|
                        @current_monitor = monitor
                        instance_eval(&block) if block
                    end
                elsif (mon = @flow.monitors(subject.to_s))
                    @current_monitor = mon
                    instance_eval(&block) if block
                end
            end
            ## new "actions" to do for monitors,
            #i.e. take the table name from the monitors itself
            #not flow
            inside_method_m = Proc.new do |monitor,type,dir|
                m = "table_" + type.to_s
                name = monitor.send(m.to_sym,dir)
                all_tables = TableUtil::search_tables name
                method_name = "#{action}_table"
                all_tables.each do |tname|
                    TableUtil.send(method_name.to_sym,tname)
                end
            end
            ## define method for the monitors part
            #r/d ,records,records_union !!
            [:records,:records_union].each do |type|
                # table_stats, table_records etc..
                define_method "table_#{type}".to_sym do |*dirs|
                    Util::starts_for(dirs) do |dir|
                        inside_method_m.call(@current_monitor,type,dir)
                    end
                end
            end
            # specail case for stats table and backlog  because no direction for this one
            [:stats,:records_backlog].each do |type|
                define_method "table_#{type}".to_sym  do 
                    meth_name = "#{action}_table"
                    table_meth = "table_#{type}".to_sym
                    table_name = @current_monitor.send(table_meth)
                    TableUtil.send(meth_name,table_name)
                end
            end
        end
    end
    self.construct(TableReset,:reset)
    self.construct(TableDelete,:delete)
end
