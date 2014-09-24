#!/usr/bin/ruby

require './config'

class Util

	##
	## Concat the path and return the full path 
	## with the BASE_DIRECTORY + DATA_DIR
	def self.data_path(*path)
		EMMConfig['BASE_DIR'] + "/" + EMMConfig['DATA_DIR'] + "/" + path.join('/')
	end

    # return the timestamp associated with
    # the filename of a cdr
    # RETURN AT THE MINUTE PRECISION
    def self.cdr2stamp(name)
        return unless name.match /_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})_/
        year = $1
        month = $2
        day = $3
        hours = $4
        min = $5
        sec = $6
        Time.gm(year,month,day,hours,min).to_i        
    end

    def self.stamp2time timestamp
        Time.at(timestamp).utc
    end
    
    ## return list of folders for
    # this type of CDR
    # folders used on the local machine in the
    # DATA_STORE_DIR / DATA_BACK_DIR / DATA_TMP_DIR
    def self.folders type
        folders = []
        case type
        when :MSS
            switches = EMMConfig["MSS_SWITCHES"]
            switches.each do |s|
                folders << EMMConfig["MSS_SWITCHES_#{s.upcase}"]
            end
            folders.flatten!
        when :gprs

        else
            folders = nil
        end
        folders
    end

end
