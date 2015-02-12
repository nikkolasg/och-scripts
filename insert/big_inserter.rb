require_relative '../config'
require_relative '../logger'
require_relative '../util'
require_relative '../ruby_util'
require_relative '../database'
require_relative '../cdr'
require_relative 'inserter'
module Inserter

    ## Inserter that insert big files
    ## write the output into a temp file and 
    #then ask MySql to load it
    class BigSourceInserter < GenericSourceInserter

        def initialize(source,infos)
            super(source,infos)
        end

        require_relative '../database/table_util'

        ## overriding the method from the generic source
        ## see comments in "inserter.rb" for more explanations
        def insert_
            nfiles = @curr_schema.select_new_files
            @count = nfiles.size
            Logger.<<(__FILE__,"INFO","Found #{nfiles.size} files to decode & insert for #{@curr_source.name}...");
            return unless nfiles.size > 0

            base_path = File.join(Conf::directories.store,@curr_source.name.to_s)
            ids = [] ## ids processed
            @file_counter = 0
            iterate_over nfiles do |file|
                file_path = File.join(base_path,file[:folder],file[:file_name])
                file_ = CDR::File.new(file_path,search: true)
                bulk_insert file_,file[:file_id]
                @curr_schema.processed_files RubyUtil::arrayize file[:file_id]
                backup_file file[:folder],file_
                @file_counter += 1
                Logger.<<(__FILE__,"INFO","(#{@file_counter}/#{@count}) Decoded #{file_.name}")

            end

            Logger.<<(__FILE__,"INFO","Decoded & Inserted #{@count} files ...")
            Logger.<<(__FILE__,"INFO","Insert operation finished !")
        end


        ## Will write everything to a file and then ask MySql to load it
        def bulk_insert file,file_id
            tmp = File.join Conf::directories.tmp,"insert_#{file.name}.data"
            ## make sure we are not taking random datas
            File.delete(tmp) if File.exists?(tmp)
            @tmp_file = File::open(tmp,"w")
            sql_fields = nil
            SignalHandler.ensure_block {@tmp_file.close; File.delete(tmp) }
            line = 0
            ## decoding
            @curr_source.decoder.decode file do |fields,record|
                sql_fields ||= fields
                write_to_file fields,record,file_id
                line += 1
                Logger.<<(__FILE__,"INFO","Decoded #{line} records from #{file.name}...",inline: true) if line % 10000 == 0
            end
            @tmp_file.close
            ## insertion
            ## with file_id before
            sql = "LOAD DATA CONCURRENT LOCAL INFILE '#{tmp}' " + 
                " INTO TABLE #{@curr_source.schema.table_records} " +
                RubyUtil::sqlize(['file_id'] + sql_fields.keys,no_quote:true)
            Database::Mysql::send_raw_sql_cmd sql
            File.delete(tmp) if File.exists?(tmp)
        end

        def write_to_file fields, record,file_id
            ## if we need to index some values before
            @curr_source.schema.index_value fields,record if @curr_source.schema.respond_to?(:index_value)
            @tmp_file.print file_id +"\t"+ record.values_at(*fields.values).join("\t")
            @tmp_file.print "\n"
        end

    end
end
