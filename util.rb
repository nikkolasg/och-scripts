#!/usr/bin/ruby

require './config/config'
require './ruby_util'
module Util
    
    # return the flow name from the file name of the cdr
    def self.flow file_name
        re = /\w+_(\w+)_\d+_\d+\.(DAT|DAT\.GZ)/
        return unless file_name.match re
        switch = $1
        flows = App.flows
        params = Hash[flows.map { |t| [t.name,t.switches]}]
        params.each  do |flow,v|
            next if !params[flow].include? switch
            return flow.upcase.to_sym
        end
        return nil
    end
    def self.switch file_name
         re = /\w+_(\w+)_\d+_\d+\.(DAT|DAT\.GZ)/
         return unless file_name.match re
         switch = $1
         switch
    end
	##
	## Concat the path and return the full path 
	## with the BASE_DIRECTORY + DATA_DIR
    # Possible to specify from which direction you are working on
    # input or output flow
    # DEPRECATED
	def self.data_path(*path)
        opts = path.last.is_a?(Hash) ? path.pop : nil
		str = EMMConfig['BASE_DIR'] + "/" + 
              EMMConfig['DATA_DIR'] + "/" 
        if opts && opts[:dir] && opts[:dir] == :output
            str << "OUT/"
        end
        str << path.join('/')
        return str
	end

    # return the interger value of a direction of a flow
    # for storing ind atabase
    # DEPRECATED
    def self.dir2int direction
        direction == :input ? 0 : 1
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
    # this flow of CDR
    # folders used on the local machine in the
    # DATA_STORE_DIR / DATA_BACK_DIR / DATA_TMP_DIR
    # DEPRECATED !!!
    def self.folders flow
        flow = flow.upcase.to_sym
        folders = []
        case flow
        when :MSS
            switches = RubyUtil::arrayize EMMConfig["MSS_HOSTS"]
            switches.each do |s|
                folders << EMMConfig["#{s.upcase}_SWITCHES"]
            end
            folders.flatten!
        when :gprs

        else
            folders = nil
        end
        folders
    end

    ## find the flow of cdr host is hosting and return its switches
    #i.e. its folder where to find the raw cdrs
    # make a check if it exists before
    # DEPRECATED !!!
    def self.switches host
       flows = RubyUtil::arrayize EMMConfig["CDR_FLOWS"]
       flows.each do |t|
            next unless EMMConfig["#{t.upcase}_HOSTS"].include? host
            return EMMConfig["#{host.upcase}_FOLDERS"]      
       end 
    end
end
