module Getter
    require_relative '../ruby_util'
    require_relative '../logger'
    require_relative '../logger'
    
    ## Class that will only record the number of files for the source
    # it will NOT download them
    # The schema will store the info as usual
    class FileCountGetter
        include Getter

        def initialize(source,infos)
            @current_source = source
            @opts = infos
            @take = infos[:take]
            @files = {}
            @current_source.set_options(@opts)
        end

        def get
            Logger.<<(__FILE__,"INFO","Starting GET opertions in #{self.class.name}..")
           get_remote_files 
           count = filter 
           if count == 0
               Logger.<<(__FILE__,"INFO","Nothing new for #{@current_source.name.to_s} :|")
               return
           end
           Logger.<<(__FILE__,"INFO","Filtering on remote files done ... will store #{count} files entries")
           @current_source.schema.insert_files @files
           Logger.<<(__FILE__,"INFO","Files inserted in the system !")
           Logger.<<(__FILE__,"INFO","GET operation finished.")
        end
    end

end
