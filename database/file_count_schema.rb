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
module Database

    module Schema

        module Source
            require_relative 'source_schema'

            class FileCountSchema  < GenericSchema

                def initialize source,opts = {}
                    @source = source
                    @opts = opts
                    @table_files = "FILES_#{source.name.upcase}"
                    @db = Database::Mysql.default
                end

                def rename new_name
                    nfiles = "FILES_#{new_name.upcase}"
                    TableUtil::rename_table @table_files,nfiles
                    Logger.<<(__FILE__,"INFO","Renamed files table #{@table_files} => #{nfiles}")
                    @table_files = nfiles
                end

                ## create the file table
                def create type
                    return if type == :records ## no records for this
                    @db.connect do

                        sql = SqlGenerator.for_files(@table_files) 
                        @db.query(sql)
                        Logger.<<(__FILE__,"INFO","Setup files table for #{@source.name}")

                    end
                end

                ## delete the file table
                def delete type
                    return if type == :records
                    @db.connect do
                        TableUtil::delete_table @table_files    
                        Logger.<<(__FILE__,"INFO","Deleted files table for #{@source.name}")
                    end
                end

                ## reset the file table
                def reset type
                    return if type == :records
                    @db.connect do 
                        TableUtil::reset_table @table_files 
                        Logger.<<(__FILE__,"INFO","Reset files table for #{@source.name}")
                    end
                end

                undef_method :select_new_files
                undef_method :insert_records

                def new_records monitor,opts= {}
                    sql = "SELECT file_id,file_name," +
                         "unix_timestamp(#{Conf::database.timestamp}) as #{Conf::database.timestamp} " +
                         "FROM #{@table_files} " +
                         "WHERE processed = 0 AND " +
                         "file_id NOT IN (SELECT file_id FROM " +
                        monitor.schema.table_records(@source) + ")" 
                    @db.connect do 
                        res = @db.query(sql)
                        opts[:proc].call(res.num_rows) if opts[:proc]
                        Logger.<<(__FILE__,"INFO","Retrieved #{res.num_rows} files to count from #{@source.name}")
                        res.each_hash do |row|
                            yield RubyUtil::symbolize(row)
                        end
                    end
                    
                end

            end

        end

    end

end