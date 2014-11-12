#!/usr/bin/ruby

require './config'
require './ruby_util'
module Util
    SEC_IN_MIN = 60
  
    DATE_FORMAT = "%Y%m%d"
   
    def self.date cuando,opts = {}
        format = opts[:format] || DATE_FORMAT
        cmd = "date '+#{format}' --date='#{cuando}'"
        o,e,s = Open3.capture3(cmd)
        unless s.success?
            Logger.<<(__FILE__,"ERROR","While getting the date #{cmd} => #{e}.")
            abort
        end
        return o.chomp
    end

   def self.array_avg arr
      arr.inject(0) { |col,ar| col += ar.size } / arr.size
   end
    ## util that handles the directions flows
    # use with blocks to execute for any directions
    def self.starts_for dir
        if dir.is_a?(Array) 
           if !(dir & [:input,:output]).empty?
            yield :input if block_given?
            yield :output if block_given?
           elsif dir.include? :both
               yield :input if block_given?
               yield :output if block_given?
           end
        elsif dir == :input || dir == :output
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
    def self.decompose time_field,granularity = :all
            # Year   Month  Day    Hours  Min    Secs
       re = /(\d{4})?(\d{2})?(\d{2})?(\d{2})?(\d{2})?(\d{2})?/
       return unless time_field.match re
        case granularity
        when :year
            return $1
        when :month
            return $1,$2
        when :day
            return $1,$2,$3
        when :hour
            return $1,$2,$3,$4
        when :min 
            return $1,$2,$3,$4,$5
        when :sec, :all
            return $1,$2,$3,$4,$5,$6
        end
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
    
end
