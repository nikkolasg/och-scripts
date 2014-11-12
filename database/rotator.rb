## Class that handles the rotation of
#any tables in the database
#Mostly used for rotating records tables
#it can also be used to rotate cdr / monitors tables
#You can pass options to this class like,
# from which date you wish to rotate the table,
# that will store all rows before this date into another
# table suffixed by a date
module Database
    # little hack to not have one more useless indentation 
    # because defining only class method (and dont want to put self.
    # everywhere !)
    class TableRotator
        YESTERDAY="yesterday"

    end
    class << TableRotator
        # do the actual rotation
        def rotate type,subject, opts = {}
            unless [:cdr,:records,:monitor].include? type
                Logger.<<(__FILE__,"ERROR","TableRotator rotation subject unknown #{subject} :(. Abort.")
                abort
            end

            self.send("rotate_#{type}".to_sym,subject,opts)
        end

        # rotate records table by days 
        # Will move the current table to a new one
        # with suffix of yesterday date
        # i.e. RECORDS_MSS_20141027
        # and also it delete and create a new MERGE
        # table containing all previous tables
        def records flow,opts
            Util::starts_for(opts[:dir]) do |dir|
                cmd = "#{Util::date(YESTERDAY)}"
                yesterday = `#{cmd}`
                # MOVE the table 
                table_name = flow.table_records(dir)
                new_name = table_name + "_" + yesterday
                TableUtil::rename_table table_name, new_name
                TableUtil::compress_table new_name 
                # re create the table
                sql = SqlGenerator.for_records(flow.table_records(dir),
                                               flow.records_fields)
                # reCREATE the MERGE 
                TableUtil::delete_table flow.table_records_union(dir)
                tables = TableUtil::search_tables table_name
                sql_ = SqlGenerator.for_records_union(flow.table_records_union(dir),
                                                     flow.records_fields,
                                                     union: tables )
                db = Mysql.default
                db.connect { db.query(sql); db.query(sql_) }
            end
        end

        def cdr flow,opts
            Util::starts_for(opts[:dir]) do |dir|
                ts = get_timestamp opts
                oldt = flow.table_cdr(dir)
                new_table = get_new_name oldt,ts
                # transfer old data to archive table
                move_and_delete oldt,new_table,ts
                # delete adn recreate table union
                table =  flow.table_cdr_union(dir)
                union_t = TableUtil::search_tables oldt
                sql = SqlGenerator.for_cdr_union table,union: union_t
                db = Mysql.default
                db.connect do
                    db.query(sql)
                end
            end
        end
        def monitor monitor,opts
            Util::starts_for(opts[:dir]) do |dir|
                ts = get_timestamp opts
                oldt = monitor.table_records(dir)
                newt = get_new_name oldt,ts
                
                move_and_delete oldt,new_table,ts
                
                table = monitor.table_records_union(dir)
                union_t = TableUtil::search_tables oldt
                sql = SqlGenerator.for_monitors_records_union table,union: union_t
                db = Mysql.default
                db.connect do 
                    db.query(sql)
                end     
            end
        end
                
        private
        # return the new name formed based on the original name + ts
        def get_new_name table,timestamp
            table + "_" + to_date(timestamp)
        end

        # make the transfer of the old and new table
        def move_and_delete oldt,newt,timestamp
            ins = "INSERT INTO #{newt} SELECT * FROM #{oldt} " +
                  " WHERE #{App.database.timestamp} < #{timestamp}";
            del = "DELETE FROM #{oldt} WHERE " +
                    " #{App.database.timestamp} < #{timestamp}"
            db = Mysql.default
            db.connect do
                db.query(ins)
                db.query(del)
            end
            TableUtil::compress_table newt
        end
        def cdr_table flow,dir
            flow.table_cdr(dir)
        end
        def monitor_records_table mon,dir
            mon.table_records(dir)
        end
        def get_timestamp opts
            year = opts[:year]
            month = opts[:month]
            day = opts[:day]
            Time.new(year,month,day).utc.to_i
        end
        def to_date timestamp
            Time.at(timestamp).strftime(Util::DATE_FORMAT)
        end

    end
end
