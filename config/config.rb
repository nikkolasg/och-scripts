#!/usr/bin/ruby
# Configuration of monitoring tool
# DSL - like.
# For more info see app.rb

require './config/app.rb'
require './logger'

App.config do 
    app_name "Emm monitoring"
    app_version 1.3
     
    # all stuff relative to the directories in the application
    directories do
        ## base directory of the app
        app "/home/ngailly/scripts"
        ## directory where all datas are stored
        data "datas"
        ## sub directories of data 
        tmp "tmp"
        store "server"
        backup "backup"
        out_suffix "_out" ## append suffix for output flow direction
    end

    flow "MSS" do 
        table_cdr "CDR_MSS"
        table_records "RECORDS_MSS"
        table_stats "STATS_MSS"
        
        source :input do
            protocol "sftp"
            host "miles"
            login "cgarnero"
            password "Asd..asd"
            base_dir "/cdr/work/archive/raw"
            switch "LSMSS10"
        end
        source :input do
            protocol "sftp"
            host "rislan"
            login "cgarnero"
            password "Asd..asd"
            switch "LSMSS10"
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
        level_email 3
        level_sms 4
    end
end

Logger.<<(__FILE__,"INFO","#{App.app_name} (v. #{App.app_version}) starting ...")
