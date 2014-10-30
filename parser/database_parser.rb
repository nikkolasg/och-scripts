require_relative '../logger'

class DatabaseParser
    require './datalayer'

    @actions = [:setup,:reset,:delete,:rotate,:dump]
    class << self
        attr_accessor :actions
        include './parser/helper'         
    end

    def self.parse(argv,opts = {})
        (Logger.<<(__FILE__,"ERROR","No action given to database utility. Abort."); abort;) unless argv.size > 0
        action = argv.shift.downcase.to_sym

        (Logger.<<(__FILE__,"ERROR","Database action unknown. Abort.");abort;) unless SetupParser.actions.include? action

        DatabaseParser.send(action,argv)
    end
    
    # create the tables used by the monitoring tool
    def self.setup argv, opts 
        flow_action = Proc.new do |flow|
            Tables::create_table_records flow
            Tables::create_tables_monitors flow
            Tables::create_tables_cdr flow
            Logger.<<(__FILE__,"INFO","Created tables for the #{flow.name} flow.");
        end
        files_action = Proc.new do |files|
            Logger.<<(__FILE__,"WARNING","No database setup for files.Abort.")
            abort
        end
       
        monitor_action = Proc.new do |monitor|
            Datalayer::Tables::create_table_monitor monitor
        end
        take_actions argv,flow_action,files_action,monitor_action 
    end
    
    # flush the tables 
    def self.reset argv, opts
        flow_action = Proc.new do |flow|
            Datalayer::Tables::reset_monitor flow
            Datalayer::Tables::reset_records flow
            Datalayer::Tables::reset_cdr flow
        end

        backlog_action = Proc.new do |files|
            Logger.<<(__FILE__,"WARNING","No reset action for backlog yet.Abort.");
            abort
        end
        monitor_action = Proc.new do |monitor|
            Datalayer::Tables::reset_monitor monitor.flow,monitor
        end
        take_actions argv,flow_action,backlog_action,monitor_action
    end
    
    # delete the tables
    def self.delete argv, opts
        flow_action = Proc.new do |flow|
            Datalayer::Tables::delete_monitor flow
            Datalayer::Tables::delete_records flow
            Datalayer::Tables::delete_cdr flow
        end
        backlog_action = Proc.new do |files|
            Logger.<<(__FILE__,"WARNING","No delete action for backlog yet.Abort.")
            abort
        end
        monitor_action = Proc.new do |monitor|
            Datalayer::Tables::delete_monitor monitor.flow,monitor
        end
    end
    
    # rotate the tables if necessary  
    def self.rotate argv, opts

    end

     def self.dump argv,opts
        (Logger.<<(__FILE__,"ERROR","No flow specified to setup : dump action . Abort."); abort) unless argv.size > 0
        flow = argv.shift.upcase.to_sym
        (Logger.<<(__FILE__,"ERROR","Flow unknown to setup : dump action. Abort."); abort;) unless App.flow(flow)
        flow = App.flow(flow)
        test_file = CDR::File.new(flow.test_file)

        require './cdr'
       
        opts = { flow: flow.name, allowed: flow.records_allowed }  
        json = test_file.decode test_file, opts
        file_name = App.directories.database_dump + "#{flow.name}_records_fields.db"
        CDR::dump_table_file json, file_name

        Logger.<<(__FILE__,"INFO","Dumped database file to #{file_name}")
    end

 
   
end
