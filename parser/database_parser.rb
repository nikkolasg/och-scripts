require_relative '../logger'
module Parser
class DatabaseParser
    require './database'
    KEYWORDS = [:database]
    @actions = [:setup,:reset,:delete,:rotate,:dump]

    require  './parser/helper'         
    class << self
        attr_accessor :actions
        include Parser
    end

    def self.parse(argv,opts = {})
        (Logger.<<(__FILE__,"ERROR","No action given to database utility. Abort."); abort;) unless argv.size > 0
        action = argv.shift.downcase.to_sym

        (Logger.<<(__FILE__,"ERROR","Database action unknown. Abort.");abort;) unless self.actions.include? action

        DatabaseParser.send(action,argv,opts)
    end

    # create the tables used by the monitoring tool
    def self.setup argv, opts 
        ah = {}
        ah[:flow] = Proc.new do |flow|
            Database::TableCreator.for(flow) do
                cdr opts[:dir]
                records opts[:dir]
                monitor :all do
                    table_stats
                    table_records opts[:dir]
                    table_records_backlog
                end
            end
        end
        ah[:cdr] = Proc.new do |flow|
            Database::TableCreator.for(flow) { cdr opts[:dir] }
        end
        ah[:records]= Proc.new do |flow|
            Database::TableCreator.for(flow) { records opts[:dir] }
        end
        ah[:monitor] = Proc.new do |mon|
            Database::TableCreator.for(mon.flow) do
                monitor mon.name do
                    table_stats
                    table_records opts[:dir]
                    table_records_backlog
                end
            end
        end
        take_actions argv,ah
    end

    # flush the tables 
    def self.reset argv, opts
        ah = {}
        ah[:flow] = Proc.new do |flow|
            Database::TableReset.for flow do 
                cdr opts[:dir]
                records opts[:dir]
                monitor :all do 
                    table_stats
                    table_records opts[:dir]
                    table_records_backlog
                end
            end
        end
        ah[:cdr] = Proc.new do |flow|
            Database::TableReset.for(flow) { cdr opts[:dir] }
        end
        ah[:record]= Proc.new do |flow|
            Database::TableReset.for(flow) do
                cdr opts[:dir] 
                records opts[:dir]
            end
        end

        ah[:monitor]= Proc.new do |mon|
            Database::TableReset.for mon.flow do
                monitor mon.name do 
                    table_stats 
                    table_records opts[:dir]
                    table_records_backlog
                end
            end
        end
        take_actions argv,ah
    end

    # delete the tables
    def self.delete argv, opts
        print = Proc.new do |subject,tables|
            Logger.<<(__FILE__,"INFO","Deleted operation terminated for #{subject.name} => #{tables.join(',')}")
        end
        ahash = {}
        ahash[:flow] = Proc.new do |flow|
            Database::TableDelete.for flow do 
                records opts[:dir]
                records_union opts[:dir]
                monitor :all  do
                    table_stats
                    table_records opts[:dir]
                    table_records_backlog
                end
                cdr opts[:dir]
            end
            print.call(flow,[:records,:table_stats,:table_records,:cdr])
        end
        ahash[:cdr] = Proc.new do |flow|
            Database::TableDelete.for(flow) { cdr opts[:dir] }
        end
        ahash[:records] = Proc.new do |flow|
            Database::TableDelete.for(flow) { records opts[:dir] }
        end
        ahash[:monitor] = Proc.new do |mon|
            Database::TableDelete.for mon.flow do 
                monitor mon.name do
                    table_stats 
                    table_records opts[:dir]
                    table_records_backlog
                end
            end
        end
        take_actions argv,ahash
    end

    # rotate the tables if necessary  
    def self.rotate argv, opts
        ah = {}
        ah[:flow] = Proc.new do |flow|
            flow.monitors.each {|m| Database::TableRotator.monitors(m,opts) }
            Database::TableRotator.records flow,opts
            Database::TableRotator.cdr flow,opts
        end
        ah[:cdr] = Proc.new do |flow|
            Database::TableRotator.cdr flow,opts
        end
        ah[:records] = Proc.new do |flow|
            Database::TableRotator.records flow,opts
        end
        ah[:monitor] = Proc.new do |monitor|
            Database::TableRotator.monitor monitor,opts
        end
        take_actions argv,ah
    end

    def self.dump argv,opts
        ah = {}
        ah[:source] = Proc.new do |source|
            file = source.test_file
            decoder = source.decoder
            out = App.directories.database_dump + "/" 
            out += "#{source.flow.name}_#{source.name}.dump"
            require './dump'
            MysqlDumper::dump decoder,[file],file: out
        end  
        take_actions argv,ah
    end



    end
end
