require_relative '../logger'
## class that will handle all
#  the setup relative to the monitoring tool
#  like create database, dumping fields files,
#  cheking directory etc
#  think of it like an UTILITY
class SetupParser
    
    @actions = [:directories,:dump,:database,:all]
    class << self; attr_accessor :actions end
    # parse the commands given to setup utility
    def self.parse(argv,opts = {})
        (Logger.<<(__FILE__,"ERROR","No action given to setup utility. Abort."); abort;) unless argv.size > 0
        action = argv.shift.downcase.to_sym
        
        (Logger.<<(__FILE__,"ERROR","Setup action unknown. Abort.");abort;) unless SetupParser.actions.include? action

        SetupParser.send(action,argv)
    end

    def self.dump argv
        (Logger.<<(__FILE__,"ERROR","No flow specified to setup : dump action . Abort."); abort) unless argv.size > 0
        flow = argv.shift.upcase.to_sym
        (Logger.<<(__FILE__,"ERROR","Flow unknown to setup : dump action. Abort."); abort;) unless App.flow(flow)
        flow = App.flow(flow)
        test_file = CDR::File.new(flow.test_file,flow,Util::switch(flow.test_file),flow.test_file)

        require './cdr'
       
        opts = { flow: flow.name, allowed: flow.records_allowed }  
        json = test_file.decode test_file, opts
        file_name = App.directories.database_dump + "#{flow.name}_records_fields.db"
        CDR::dump_table_file json, file_name

        Logger.<<(__FILE__,"INFO","Dumped database file to #{file_name}")
    end
    def self.dump_usage
        str ="
        Action: dump
        Parameter: flow
        It will create the file containing the fields to keep in the records for this flow. Ex. MSS => calling_number,called_number etc.The file will be in formatted like column_name:SQL TYPE."
        str
    end
        
    def self.directories argv
        require './checkDir'
    end

    def self.directories_usage
        "directiries..."
    end

    def self.database argv
        require './create_tables'
        App.flows.each do |flow|
            Tables::create_table_cdr flow
            Tables::create_table_records flow
            Tables::create_table_monitors flow
            Logger.<<(__FILE__,"INFO","Created tables for the #{flow.name} flow...")
        end
    end
    def self.database_usage
        "database"
    end

    def self.all argv

    end
    def self.all_usage
         "all..."
    end

    def self.usage
        str = []
        SetupParser.actions.each do |action|
            meth = action + "_usage"
            str << SetupParser.send(meth.to_sym)
        end
        str
    end
end
