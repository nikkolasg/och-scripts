#!/usr/bin/ruby
# Configuration of monitoring tool
# DSL - like.
# For more info see app.rb

require_relative 'config/app'
require_relative 'ruby_util'

Conf.make do 

    app "Emm monitoring"  do
        version 1.4
        author "Nicolas GAILLY" 
        contributers "De Lalene Diener Jan Raino Eerik", "Chrystelle Garnero"
    end

    # one host defined in the system
    host "emm_crissier" do
        address "10.23.3.20"
        login "mmsuper"
        password "mediation"
        protocol :sftp
    end

    host "emm_zurich" do 
        address "10.23.11.20"
        login "mmsuper"
        password "mediation"
        protocol :sftp
    end
    
    host "ubu15" do
        address "ubu15"
        login "ngailly"
        password "garnerozebest"
        protocol :sftp
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
        level_email 4
        level_sms 5
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

        database_dump "dump" # table schema file will be dumped here
        # format column_name: SQL TYPE
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

    ## MAIN SECTIONS HERE ##
    ## Define a "FLOW" that contains many "sources"
    ## that we can compare with different "monitors"
    #that does the statistics we want
    flow "MSS" do 
        # which fields represents the best the time to sort the file on
        time_field_records "charging_start_time" 

        # Filter out the records we want for this flow
        # youc an filter a field and specify a "block" which
        # must return true or false.
        # WARNING: Every field NOT specified in a filter, will be rejected
        # by this latter. WITH ONE EXCEPTION is the time_field_records will
        # always be included
        filter do
            fields_file "MSS_records_fields.db" 
            # only take POC,MOC,and FORW records
            field (:record_type) { |x| [1,3,11].include? x.to_i }
        end

        
        source "mss_out" do 
            host "emm_crissier"
            base_dir "/var/opt/mediation/MMStorage/ARCHIVAL/DIST/MSS/QVANTEL"
            folders *%w(LSMSS10 LSMSS11 LSMSS30 LSMSS31 ZHMSS20 ZHMSS21 FLMSS01)

            # the Decoder that must be used for the files at the source
            decoder :CSVDecoder,type: :MSS,separator: ";"
            # a custom mapper can be specifed here
            # a mapper will work on the output of the decoder
            # and change the name of fields, or regroup multiple fields
            # into one so it fits for this flow specifications
            # it TRANSFORMS the datas from the file into the "flow area"
            # You have to specify the classs name and the file containing
            # the class must be place in mapper/ folder.
            # The class itelf must be declared  within the Mapper:: module
            mapper :Nkm15Mapper ## default one, let everything as it is
            ## The class that will handle all the transactions in the database,
            #handle its own schema ! =) default is GenericSchema,
            #one table for cdr, one for records.
            schema :GenericSchema

        end

        source "mss_in" do
            host "emm_crissier"
            base_dir "/var/opt/mediation/MMStorage/ARCHIVAL/RAW/MSS"
            folders *%w(LSMSS31 ZHMSS20 ZHMSS21 LSMSS10 LSMSS11 LSMSS30 FLMSS01)
            decoder :NKM15Decoder
            mapper :Nkm15Mapper
            # Options for the file_manager of this source so it knows where 
            # to search for the files. Here are the defaults. 
            # Look at file_manager.rb for more infos.
            file_manager_options subfolders: true, 
                min_date: "today" 

            # no mapper needed here since identity mapper is the default
        end

        eval(IO.read(File::dirname(__FILE__) + '/config_monitor_mss.rb'),binding)

    end

    flow "MMS" do 
    
        time_field_records :submit_date

        filter do
            ## only take Send or Receive CDR
            field (:action) { |x| ["S","R"].include?(x) }
            fields :message_class_size,:message_type,:action_final_state
            fields :message_size,:message_id,:owner,:submit_date
            fields :final_state_date,:recipient_types
            fields :handset_type,:operator_id,:transaction_id,:original_sender
            fields :a_imsi,:b_imsi,:a_number,:b_number,:a_mail,:b_mail
        end

        source "mms_in" do
            host "emm_crissier"
            base_dir "/var/opt/mediation/MMStorage/ARCHIVAL/RAW/MMSC"
            folders "COMVS11","COMVS12"
            decoder :TALDecoder
            mapper :TalMapper
            file_manager_options file_regexp: nil
        end
        source "mms_out" do
            host "emm_crissier"
            base_dir "/var/opt/mediation/MMStorage/ARCHIVAL/DIST/MMSC/QVANTEL"
            folders "COMVS1"
            decoder :CSVDecoder,type: :MMS
            mapper :MmsOutMapper
            file_manager_options file_regexp: nil
        end

        eval(IO.read(File::dirname(__FILE__) + "/config_monitor_mms.rb"),binding)

    end

    flow "SMS" do
       
      time_field_records :submit_date
      filter do
          field :reference_id,:mmsc_id,:a_imsi,:b_imsi,:tariff_class,:vmsc_number,:sid, :submit_date
          fields_file "SMS_records_field.db"
      end 

        source "sms_in" do
            host "emm_crissier"
            base_dir "/var/opt/mediation/MMStorage/ARCHIVAL/RAW/SMSC"
            folders "LSMOS11","LSMOS21","LSMOS31","LSMOS41","LSMOS51"
            decoder :LogicaDecoder
            mapper :LogicaMapper
            file_manager_options file_regexp: nil
        end

        source "sms_out" do
            host "emm_crissier"
            base_dir "/var/opt/mediation/MMStorage/ARCHIVAL/DIST/SMSC"
            folders "QVANTEL" ## no folders by switch -_-"
            decoder :CSVDecoder, type: :SMS, separator: ";"
            mapper :SmsMapper
            
        end
        
        eval(IO.read(File::dirname(__FILE__) + "/config_monitor_sms.rb"),binding)

    end

    flow "data" do


    end

end

