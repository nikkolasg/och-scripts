## Module that will handle the mapping of 
# file / records into a "standard" / normalized form
# can regroup fields into one, can rename one field,
# can change the type of values etc 
module Mapper

    def self.create klass_name, opts ={}
        if klass_name.is_a?(String)
            klass_name = klass_name.capitalize.to_sym
        end
        return nil if klass_name == :GenericMapper ## No need to even create it
        mapper = Mapper::const_get(klass_name).new(opts)
    end

    # does nothing, simply output the same
    class GenericMapper
        def initialize opts = {}
            @opts = opts
        end


        ## transform the litterals time values i
        # into integer timestamp =)
        # fields is expected to be vars args
        # of index
        # select only time related time !
        # row is a row in the json
        def transform_time_values row,*fields 
            fields.each do |i|
                next unless i ## workaround, same mapper used for different format
                              ## thens some fields may not exists
                v = row[i]
                values = Util::decompose(v,:sec).compact
                if values.empty? 
                    row[i] = 0
                else
                    begin
                    row[i] = Time.utc(*values).to_i     
                    rescue => e
                        puts values.inspect
                        puts e
                        abort
                    end
                end
            end
            return true
        end
        ## rename the time fields in the array "fields"
        # and return a hash to be used with renamed field
        # Key : old field name 
        # Value : new field name ==> "ts_" + old_field
        def map_time_fields fields
            fields.inject({}) do |col,field|
                nfield = Util::TIME_PREFIX + field.to_s
                col[field] = nfield.to_sym
                col
            end
        end
        ## Pass an hash of fields (fields => index)
        ## AND a hash of NEW FIELDS (old field name => new field name)
        def rename_fields fields, newfields
            newfields.each do |of,nf|
                v = fields.delete(of)
                next unless v
                fields[nf] = v
            end
            return fields
        end

    end

    Dir[File::dirname(__FILE__) + "/*.rb"].each do |f|
        require_relative "#{f}"
    end




end
