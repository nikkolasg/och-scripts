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
            require_relative '../database'
            db = Database::Mysql.default
            db_info = {}
            db.connect do
                db_info = Database::TableUtil::list_tables(db).inject({}) do |col,table|
                    col[table] = Database::TableUtil::list_fields(table,db)
                    col
                end
            end
            Conf::flows.each do |flow|
                flow.sources.each do |source|
                    ## CHECK CDR
                    unless check_table db_info,source.schema.table_files
                        if opts[:mock]
                            Logger.<<(__FILE__,"INFO","MOCK : database setup files #{source.name}")
                        else
                        DatabaseParser::parse(["setup" ,"files","#{source.name}"])
                        end
                    end
                    CheckParser::WELL.call("db:files #{source.name}")
                    ## CHECK RECORDS
                    unless check_table db_info,source.schema.table_records,source.records_fields
                        if opts[:mock]
                            Logger.<<(__FILE__,"INFO","MOCK : database setup records #{source.name}")
                        else
                        DatabaseParser::parse(["setup","records", "#{source.name}"])
                        end
                    end
                    CheckParser::WELL.call("db:records #{source.name}")
                end

                ## MONITOR SECTION
                flow.monitors.each do |mon|
                    schema = mon.schema
                    cmd = ["setup", "monitor","#{mon.name}"]
                    ## CHECK STATS
                    fields = schema.stats_columns.inject({}) {|col,f| col[f] = "INT DEFAULT 0"; col }
                    unless check_table db_info,schema.table_stats,fields
                        if opts[:mock]
                            Logger.<<(__FILE__,"INFO","MOCK : #{cmd}")
                        else
                        DatabaseParser::parse(cmd)
                        end
                    end
                    CheckParser::WELL.call("db:monitor:stats #{mon.name}")

                    flow.sources.each do |source|
                        ## CHECK BACKLOG
                        unless check_table db_info, schema.table_records(source,backlog:true)
                            if opts[:mock]
                            Logger.<<(__FILE__,"INFO","MOCK : #{cmd}")
                            else
                            DatabaseParser::parse(cmd)
                            end
                        end
                        CheckParser::WELL.call("db:monitor:records:backlog #{mon.name}")
                        ## CHECK MON_RECORDS
                        unless check_table db_info,schema.table_records(source)
                            if opts[:mock]
                                Logger.<<(__FILE__,"INFO","MOCK : #{cmd}")
                            else
                            DatabaseParser::parse(cmd)
                            end
                        end
                        CheckParser::WELL.call("db:monitor:records #{mon.name}")
                    end
                end
            end
        end

        def dir argv,opts
            if opts[:mock]
                Logger.<<(__FILE__,"INFO","MOCK : directories setup ")
            else
            DirectoriesParser::parse(["setup"],opts)
            end
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
            d = Conf::directories
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
            db = Conf::database
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
            lg = Conf::logging
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
            [:time_field_records,:filter].each do |f|
                unless flow.respond_to?(f)
                    Logger.<<(__FILE__,"ERROR","Flow section #{flow.name} missing #{f} field.Abort")
                    abort
                end
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
            [:host,:base_dir,:folders,:decoder].each do |f|
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
            [:sources,:time_interval].each do |f|
                unless mon.send(f)
                    Logger.<<(__FILE__,"ERROR","Monitor section #{mon.name} missing #{f} field.Abort.")
                    abort
                end
            end
            unless mon.send(:stats)
                Logger.<<(__FILE__,"ERROR","Monitor section #{mon.name} has no stats associated.Abort.")
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
            Logger.<<(__FILE__,"ERROR","DUring DB fields_ok? operation #{table} . #{e}")
            return false
        end

    end 
end