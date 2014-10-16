#!/usr/bin/ruby
# Configuration of monitoring tool
# DSL - like.
# For more info see app.rb

require './config/app.rb'
require './logger'
require './ruby_util'
App.config do 
    app_name "Emm monitoring"
    app_version 1.3
     
    # all stuff relative to the directories in the application
    directories do
        ## base directory of the app
        app "/home/ngailly/scripts"
        ## directory where all datas are stored
        data "data"
        ## sub directories of data 
        tmp "tmp"
        store "server"
        backup "backup"
        out_suffix "_out" ## append suffix for output flow direction

        database_dump "database/" # table schema file will be dumped here
                                  # format column_name: SQL TYPE
    end

    flow "MSS" do 
        
        decoder "decoding/newpmud.pl -NKM15RAW -F"

        table_cdr "CDR_MSS"
        table_records "RECORDS_MSS"
        out_suffix "_OUT" ## append suffix for table name for
                          # output flow direction 
       
        # what kind of records we want to analyze 
        records_allowed "POC","MOC","FORW"

        records_fields_file "database/MSS_records_fields.db" 
        
        test_file "test/NOKIA_LSMSS10_20140915221135_0723.DAT"
        
        source :miles do
            direction :input
            protocol "sftp"
            host "miles"
            login "cgarnero"
            password "Asd..asd"
            base_dir "/cdr/work/archive/raw"
            switch "LSMSS10","LSMSS11","LSMSS30"
        end

        source "rislan" do
            direction :input
            protocol "sftp"
            host "rislan"
            login "cgarnero"
            password "Asd..asd"
            switch "LSMSS31","ZHMSS20","ZHMSS21"
            base_dir "/cdr/work/archive/raw"
        end

        source "emm_crisser" do
            direction :output
            protocol :sftp
            host "10.23.3.20"
            login "mmsuper"
            password "mediation"
            base_dir "/var/opt/mediation/MMStorage/ARCHIVAL/RAW/MSS"
            switch "LSMSS31","ZHMSS20","ZHMSS21","LSMSS10","LSMSS11","LSMSS30"
            #sub_folders 3 # inside each switch folders, the cdr are organized into subfolders. This option specify the last N subfolder the monitoring tool must search into. It will take the N more recent ones.
        end
        
        ## create a "monitor", that will regroup the important info
        #together. A table will be named for each monitor, like
        # MONITOR_MSS_BY_DEFAULT
        monitor "by_default" do
            input "miles"
            output "emm"
            time_interval 30.minute
        end
    end
    
    database do
        host "localhost"
        name "nicotest"
        login "ngailly"
        password "simonette2014"
        ## the timest value used trhoughout multiple tables
        timestamp "timest"
    end

    logging do 
        stdout true
        log_dir "log"
        levels 1 => "DEBUG", 2 => "INFO" 
        levels 3 => "WARNING" 
        levels 4 => "ERROR"
        levels 5 => "CRITICAL"
        ## for which level we take specific actions
        level_log 2
        level_email 5
        level_sms 5
    end
end

