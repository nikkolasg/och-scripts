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
            unless [:source,:monitor].include? type.to_sym
                Logger.<<(__FILE__,"ERROR","TableRotator rotation subject unknown #{subject} :(. Abort.")
                abort
            end
            self.send(type,subject,opts)
        end

        # rotate records table by days 
        # Will move the current table to a new one
        # with suffix of yesterday date
        # i.e. RECORDS_EMM_INPUT_20141027
        # and also it delete and create a new MERGE
        # table containing all previous tables
        def source source,opts
            schema = source.schema
            yesterday = "#{Util::date(TableRotator::YESTERDAY)}"
            # MOVE the table 
            table_name = schema.table_records
            new_name = table_name + "_" + yesterday
            TableUtil::rename_table table_name, new_name
            TableUtil::compress_table new_name 
            # re create the table
            sql = SqlGenerator.for_records(schema.table_records,
                                           source.records_fields)
            # reCREATE the MERGE 
            TableUtil::delete_table schema.table_records_union
            tables = TableUtil::search_tables table_name
            sql_ = SqlGenerator.for_records_union(schema.table_records_union,
                                                  source.records_fields,
                                                  union: tables )
            db = Mysql.default
            db.connect { db.query(sql); db.query(sql_) }
        end

        private
        # return the new name formed based on the original name + ts
        def get_new_name table,timestamp
            table + "_" + to_date(timestamp)
        end

        # make the transfer of the old and new table
        def move_and_delete oldt,newt,timestamp
            ins = "INSERT INTO #{newt} SELECT * FROM #{oldt} " +
                " WHERE #{Conf::database.timestamp} < #{timestamp}";
            del = "DELETE FROM #{oldt} WHERE " +
                " #{App.database.timestamp} < #{timestamp}"
            db = Mysql.default
            db.connect do
                db.query(ins)
                db.query(del)
            end
            TableUtil::compress_table newt
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
