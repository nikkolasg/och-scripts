module Inserter
    require_relative '../debugger'
    
    ## handles the inserter of the output of EMM for the PGW flow
    ##
    #
    #WARNING NOT FINISHED !! Actually there is a PROBLEM if we want
    #to concat all files, since we need for each record the file_id to
    #where it came from. If we concat directly we lose that information.
    #One way could be to do it could be by prepending the file_id in each line
    #but there performance gain may diminish to a point near zero ....
    class PGWInserter
        include Inserter

        def initialize(source,infos)
            @v = infos[:v]
            @source = source
            @schema = source.schema
            @db = Database::Mysql.default
            @opts = infos
            @source.deocder.opts.merge! @opts
        end

        def insert
            @db.connect do 
                @source.set_db @db
                nfiles = @schema.select_new_files
                count = nfiles.size
                Logger.<<(__FILE__,"INFO","Found #{nfiles.size} files to decoder & insert for #{@source.name} ...")
                return nfiles.size > 0

                base_path = Conf::directories.store + "/" + @source.name.to_s
                ids_processed = []
                file_counter = 0

                concat_file = concat_files nfiles
                json = @source.decoder.decode concat_file
                @schema.insert_records 
            end
        end
        
        def concat_files files
            concat_file = Conf::directories.store + "/" + "pgw_concat"
            base_path = Conf::directories.store + "/" + @source.name.to_s
            ## partition in case we have to many files
            RubyUtil::partition files do |sub|
                cmd = "cat " + sub.map{|n| CDR::File.new(n[:folder]+"/"+n[:file_name],search: true).full_path }.join(" ")
                cmd += " >> \"#{concat_file}\""
            end
            CDR::File.new(concat_file,search: true)
        end

    end

end
