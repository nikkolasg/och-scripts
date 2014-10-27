#!/usr/bin/ruby

require './config/config'
require './ruby_util'
module Util
    SEC_IN_MIN = 60

    def self.starts_for dir
        if dir == :input || dir == :output
            yield dir if block_given?
        elsif dir == :both
            yield :input if block_given?
            yield :output if block_given?
        end
    end

    # return the flow name from the file name of the cdr
    # can return the Flow object directly if specified with return: :class
    def self.flow (file_name,opts = {})
        switch = Util::switch (file_name)
        flows = App.flows
        params = Hash[flows.map { |t| [t,t.switches]}]
        params.each  do |flow,v|
            next if !params[flow].include? switch
            if opts[:return] 
                if opts[:return] == :class 
                    return flow
                elsif opts[:return] == :name
                    return flow.name
                end
            end
            return flow.name
        end
        return nil
    end

    def self.switch file_name
         re = /\w+_(\w+)_\d+_\d+\.(DAT|DAT\.GZ)/
         return unless file_name.match re
         switch = $1
         switch
    end

    # decompose time field into 
    # year month day etc.. 
    def self.decompose time_field
            # Year   Month  Day    Hours  Min    Secs
       re = /(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/
       return unless time_field.match re
       return $1,$2,$3,$4,$5,$6
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
