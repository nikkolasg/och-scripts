## Class that handles the rotation of
#any tables in the database
#Mostly used for rotating records tables
#it can also be used to rotate cdr / monitors tables
#You can pass options to this class like,
# from which date you wish to rotate the table,
# that will store all rows before this date into another
# table suffixed by a date
module Datalayer

    class << TableRotator
        @@format = "%Y%m%d"
        @@date_yesterday = "date '+#{@@format}' --date='yesterday'"


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
        def rotate_records flow,opts
            [:input,:output].each do |dir|
                yesterday = `#{TableRotator.date_yesterday}`
                # MOVE the table 
                table_name = flow.table_records(dir)
                new_name = table_name + "_" + yesterday
                Tables::rename_table table_name, new_name

                # CREATE the MERGE 
                Tables::delete_table flow.table_records_union(dir)
                tables = Tables::search_tables table_name
                sql = SqlGenerator.for_records_union flow.table_records_union(dir),union: tables 
                db = MysqlDatabase.default
                db.connect { db.query(sql) }
            end
            ## CREATE the new empty tables
            Tables::create_table_monitors flow
        end

        class_eval do
            [:cdr,:monitor_records].each do |type|
                define_method "rotate_#{type}" do |subject,opts|
                    [:input,:output].each do |dir|
                        timestamp = get_timestamp opts
                        app_ts = App.database.timestamp
                        table_name = TableRotator.send("#{type}_table".to_sym,subject)
                        new_table = table_name + "_" + to_date(timestamp)

                        ## MOVE the rows
                        ins = "INSERT INTO #{new_table} SELECT * FROM #{table_name} " +
                            "WHERE #{app_ts} < #{timestamp}"
                        ## DELETE from original
                        del = "DELETE FROM #{table_name} WHERE #{app_ts} < #{timestamp}"
                        # update the new merge
                        Tables::delete_table subject.table_records dir
                        tables = Tables::search_tables table_name
                        create_union = SqlGenerator.send("for_#{type}_union".to_sym,dir)
                        db = MysqlDatabase.default
                        db.connect do
                            db.query(ins) # insert old rows 
                            db.query(del) # delete them in current table
                            db.query(sql) # re recreate union
                        end
                    end
                end
            end
        end

        private
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
            Time.at(timestamp).strftime(@@format)
        end

    end
end
