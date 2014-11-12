#!/usr/bin/ruby
# Configuration of monitoring tool
# DSL - like.
# For more info see app.rb

require './config/app'
require './ruby_util'

App.config do 

    app_name "Emm monitoring"
    app_version 1.4
            
     
    # one host defined in the system
    host "emm_crissier" do
        address "10.23.3.20"
        login "mmsuper"
        password "mediation"
        protocol :sftp
    end

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
        output_suffix "_out" ## append suffix for output flow direction

        database_dump "database/" # table schema file will be dumped here
                                  # format column_name: SQL TYPE
    end

    flow "MSS" do 
        
        # what kind of records we want to STORE 
        records_allowed "POC","MOC","FORW"

        records_fields_file "MSS_records_fields.db" 
        
        test_file "test/NOKIA_LSMSS10_20140915221135_0723.DAT"
        
        # which fields represents the best the time to sort the file on
        time_field_records "charging_start_time" 

        source "emm_output" do 
            direction :output
            host "emm_crissier"
            base_dir "/var/opt/mediation/MMStorage/ARCHIVAL/DIST/MSS/QVANTEL"
            switch *%w(LSMSS10 LSMSS11 LSMSS30 LSMSS31 ZHMSS20 ZHMSS21 FLMSS01)
            #switch "LSMSS10"

            # separator is optional
            decoder :CSVDecoder,separator: ";"
            options subfolders: true, # optionnal as default value
                    min_date: "today"
        end

        source "emm_input" do
            direction :input
            host "emm_crissier"
            base_dir "/var/opt/mediation/MMStorage/ARCHIVAL/RAW/MSS"
            switch "LSMSS31","ZHMSS20","ZHMSS21","LSMSS10","LSMSS11","LSMSS30","FLMSS01"
            #switch "LSMSS10"
            decoder :NKM15Decoder

            options subfolders: true, 
                    min_date: "today" 
        end
        
        monitor "types" do 
            input "emm_input"
            output "emm_output"
            time_interval 1.hours
        end

        monitor "world_call" do
            input "emm_input"
            output "emm_output"
            time_interval 1.hours
            filter_where :called_number_ton do |f|
               res = f == "05"
               puts "called_number_ton #{f} => #{res}"
               res
            end
            filter_where :called_number do |f|
               res = !f.start_with?("41")
               puts "called_number #{f} => #{res}"
               res
            end 
        end
        
    end
    
    database do
        host "localhost"
        name "nicotest"
        login "ngailly"
        password "simonette2014"
        ## the timest value used trhoughout multiple tables
        timestamp "timest"
        output_suffix "_OUT"
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

