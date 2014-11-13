## Class that will handle
#the parsing of the "check" operation
#basic one : check ==> will check everything (db(table + column) + fs ..)
#
module Parser
    class CheckParser

        KEYWORDS = [:check]
        @actions = [:db,:dir,:config]
        WELL = Proc.new do |name|
            Logger.<<(__FILE__,"INFO","Checking on #{name} done !")
        end
    end
    class << CheckParser
        def parse argv,opts
            action = argv.size > 0 ? argv.shift.downcase.to_sym : nil
            all(opts) unless action ## no action means check EVERYTHING
            unless @actions.include? action
                Logger.<<(__FILE__,"ERROR","Action unknown #{action}. Abort")
                abort
            end
            send(action,argv,opts)
        end
        # run every methods listed in actions
        def all opts
            @actions.each do |action|
                send(action,opts)
            end
        end

        # check if everything is present in the db regarding the configuration file
        # i.e. tables, columnns etc
        def db argv,opts
            require './database'
            db = Database::Mysql.default
            db_info = {}
            db.connect do
                db_info = Database::TableUtil::list_tables(db).inject({}) do |col,table|
                    col[table] = Database::TableUtil::list_fields(table,db)
                    col
                end
            end
            App.flows.each do |flow|
                Util::starts_for(:both) do |dir|
                    ## CHECK CDR
                    unless check_table db_info,flow.table_cdr(dir)
                        DatabaseParser::parse(["setup" ,"cdr","#{flow.name}"],{dir: dir})
                    end
                    CheckParser::WELL.call("db:cdr #{flow.name}")
                    ## CHECK RECORDS
                    unless check_table db_info,flow.table_records(dir),flow.records_fields
                        DatabaseParser::parse(["setup","records", "#{flow.name}"],{dir: dir})
                    end
                    CheckParser::WELL.call("db:records(#{dir}) #{flow.name}")

                    ## MONITOR SECTION
                    flow.monitors.each do |mon|
                        cmd = ["setup", "monitor","#{mon.name}"]
                        dir_ = { dir: dir }
                        ## CHECK STATS
                        unless check_table db_info,mon.table_stats,mon.stats_columns
                            DatabaseParser::parse(cmd,dir_)
                        end
                        CheckParser::WELL.call("db:monitor:stats #{mon.name}")

                        ## CHECK BACKLOG
                        unless check_table db_info, mon.table_records_backlog
                            DatabaseParser::parse(cmd,dir_)
                        end
                        CheckParser::WELL.call("db:monitor:records:backlog #{mon.name}")
                        ## CHECK MON_RECORDS
                        unless check_table db_info,mon.table_records(dir)
                            DatabaseParser::parse(cmd,dir_)
                        end
                        CheckParser::WELL.call("db:monitor:records(#{dir}) #{mon.name}")
                    end
                end
            end
        end

        def dir argv,opts
            DirectoriesParser::parse(["setup"],opts)
        end

        def config argv,opts
            check_database
            check_directories
            check_logging
            App.flows.each do |f|
                check_flow f
            end
        end
        private

        ############################
        ###### CONFIG SECTION ######
        ############################
        def check_directories
            d = App.directories
            [:app,:data,:tmp,:store,:backup].each do |f|
                unless d.respond_to? f
                    Logger.<<(__FILE__,"ERROR","DIrectories section missing #{f} field. Abort")
                    abort
                end
            end
            CheckParser::WELL.call("config:directories") 
        end

        def check_database
            err = Proc.new do |field|
                Logger.<<(__FILE__,"ERROR","Database section missing #{field} field. Abort.")
                abort
            end
            db = App.database
            [:host,:name,:login,:password].each do |f|
                err.call(f) unless db.respond_to?(f)
            end
            CheckParser::WELL.call("config:database")
        end
        def check_logging
            err = Proc.new do |field|
                Logger.<<(__FILE__,"ERROR","Logging section missing #{field} field. Abort.")
                abort
            end
            lg = App.logging
            [:log_dir,:level_log,:level_email,:level_sms,:log_level].each do |f|
                err.call(f) unless lg.respond_to?(f)
            end
            if lg.log_level.empty?
                Logger.<<(__FILE__,"ERROR","Logging section : no levels sets(ex. 1=> 'DEBUG'...). Abort.")
                abort
            end
            CheckParser::WELL.call("config:logging")
        end
        def check_flow flow
            [:records_allowed,:time_field_records].each do |f|
                unless flow.respond_to?(f)
                    Logger.<<(__FILE__,"ERROR","Flow section #{flow.name} missing #{f} field.Abort")
                    abort
                end
            end
            unless flow.respond_to?(:records_fields) || flow.respond_to?(:records_fields_file)
                Logger.<<(__FILE__,"ERROR","Flow section #{flow.name} missing records fields section (or records_fields_file!). Abort.")
                abort
            end
            flow.sources.each do |s|
                check_source s
            end
            flow.monitors.each do |m|
                check_monitor m
            end
            CheckParser::WELL.call("config:flow #{flow.name}")
        end
        def check_source source
            [:direction,:host,:base_dir,:switches,:decoder].each do |f|
                unless source.respond_to?(f)
                    Logger.<<(__FILE__,"ERROR","Source section #{source.name} missing #{f} field. Abort.")
                    abort
                end
            end
            [ source.host,source.decoder].each do |n|
                unless n
                    Logger.<<(__FILE__,"ERROR","Source section #{source.name} host/decoder unknown")
                    abort
                end
            end
            CheckParser::WELL.call("config:flow:source #{source.name}")
        end
        def check_monitor mon
            [:input,:output,:time_interval].each do |f|
                unless f
                    Logger.<<(__FILE__,"ERROR","Monitor section #{mon.name} missing #{f} field.Abort.")
                    abort
                end
            end
            unless mon.filter_records.size > 0
                Logger.<<(__FILE__,"ERROR","Monitor section #{mon.name} has no records_fields associated.Abort.")
                abort
            end
            CheckParser::WELL.call("config:flow:monitor #{mon.name}")
        end
        ######################
        ##### DB SECTION #####
        ####################3
        # check and modify table if needed
        # if no present return false
        def check_table db_info,table,fields = nil
            if table_ok? db_info, table # table xists
                fields_ok? table,fields if fields
            else # table dont exists
                return false
            end
            return true
        end
        ## CHECK for a table ==>
        #  1. existence, has it been created
        def table_ok? db_info,table
            db_info.keys.include? table
        end
        #  2. fields =>does it contains every fields ?
        #               if no, add the absent ones
        def fields_ok? table,fields
            table_fields = Database::TableUtil::list_fields(table)
            remainings = fields.keys - table_fields
            return true if remainings.empty?
            # add remainings fields
            remainings.each do |f|
                Database::TableUtil::add_field(table,f,fields[f])
            end
            return true
        rescue => e
            Logger.<<(__FILE__,"ERROR","DUring DB fields_ok? operation #{table}")
            return false
        end

    end 
end
