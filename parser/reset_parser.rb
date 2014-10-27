## class that will
# handle all reset operations
# if you want to reset something
class ResetParser
    require_relative '../util'
    require_relative '../get/fetchers'
    require_relative '../datalayer'
    @actions = [:cdr,:records,:process,:database,:monitors,:backup]

    require './parser/helper'
    class << self; attr_accessor :actions end

    def self.parse argv,opts
        action = argv.shift.downcase.to_sym
        (Logger.<<(__FILE__,"ERROR","Reset: Unknown action to reset.Abort.");abort;) unless ResetParser.actions.include? action

        ResetParser.send(action,argv,opts)
    end
    def self.database argv,opts
        def self.delete_flow_tables flow
               ResetParser.delete_table flow.table_cdr
               ResetParser.delete_table flow.table_cdr(:output)
               ResetParser.delete_table flow.table_records
               ResetParser.delete_table flow.table_records(:output)
               flow.monitors.each do |m|
                   ResetParser.delete_table m.table
               end
        end 
        (Logger.<<(__FILE__,"ERROR","Not Enough argument for reset database.Must specify which database you want to delete. specify all if you want to delete everything");abort;) unless argv.size > 0
        type,sub = Parser::parse_subject argv
        str = "Operation reset on database tables ..."
        case type
        when :all
            App.flows.each do |flow|
                delete_flow_tables flow
            end
        when :flow
            delete_flow_tables sub
        else
            "youhat?"        
        end
    end

    def self.cdr argv,opts
        def self.cdr_reset_flow flow,opts
            fetcher = Fetchers::create(:LOCAL,{})
            base_path = App.directories.store(opts[:dir])
            flow.switches.each do |switch|
                path = base_path + "/" + switch
                fetcher.delete_files_from path
            end
            reset_table flow.table_cdr(opts[:dir])
        end
        str = "Operation reset cdr on " 
        type,sub = Parser::parse_subject argv
        case type
        when :all
            App.flows.each do |flow|
                cdr_reset_flow flow,opts
            end
            str << "all flows done !"
        when :flow
            cdr_reset_flow sub,opts
            str << "flow #{sub.name} done!"
        when :files
            fetcher = Fetchers::create(:LOCAL,{})
            flow_name = Util.flow(sub.first)
            flow = App.flow(flow_name)
            raise "UNKNWN FLOW FOR RESET PARSER" unless flow
            reset_files_entries flow.table_cdr,sub 
            sub.each do |file|
                fetcher.delete_file file
            end
            str << "files done !"
        end
        Logger.<<(__FILE__,"INFO",str)
    end
    def self.records argv,opts
        str = "Operation reset records on "
        type,sub = Parser::parse_subject argv
        case type
        when :all
            App.flows.each do |flow|
                reset_table flow.table_records
            end
            str << " all flows done !"
        when :flow
            reset_table sub.table_records
            str << "flow #{sub.name} done!"
        when :files
            flow_name = Util.flow(sub.first)
            flow = App.flow(flow_name)
            (Logger.<<(__FILE__,"ERROR","Unknown flow for files reset operation. Abort"); abort;) unless flow
            reset_files_entries flow.table, sub
            str << "files done!"
        end
        Logger.<<(__FILE__,"INF",str)
    end     

    def self.monitors argv,opts
        str = "Operation reset monitors on "
        type,sub = Parser::parse_subject argv
        case type
        when :all
            App.flows.each do |flow|
                flow.monitors.each do |m|
                    reset_table m.table
                end
            end
            str += " all flows done !"
        when :flow
            sub.monitors.each do |m|
                reset_table m.table
            end
            str += " flow #{sub.name} done !"
        end
    end

    def self.backup argv,opts
        str = "Operation reset backup folders on"
        type,sub = Parser::parse_subject argv
        fetcher = Fetchers::create(:LOCAL,{})
        base = App.directories.backup(opts[:dir])
        case type
        when :all
            App.flows.each do |flow|
               flow.switches.each do |switch|
                   path = base + "/" + switch
                  fetcher.delete_files_from(path)
               end
            end
        when :flow
            sub.switches.each do |switch|
                path = base + "/" + switch
                fetcher.delete_files_from(path)
            end
        end
    end


    private
    def self.reset_table table
        sql = "DELETE FROM #{table};"
        db = Datalayer::MysqlDatabase.default
        db.connect do 
            db.query(sql)
        end
    end
    def self.delete_table table
        sql = "DROP TABLE #{table};"
        db = Datalayer::MysqlDatabase.default
        db.connect do 
            db.query sql
        end
    end
    def self.reset_file_entries table, entries
        sql = "DELETE FROM #{table} WHERE file_name IN "
        sql << "(" << entries.map{|f| "'#{f}'"}.join(',') << ");"
        db = Datalayer::MysqlDatabase.default
        db.connect do
            db.query(sql)
        end
    end


end
