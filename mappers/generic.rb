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
        # TODO 
    class ProceraMapper

        # return a new array of fields mapped for the 
        # application
        # # MUST BE CALLED BEFORE map_row so it knows the operations 
        # to do on the row
        def map_fields fields

        end

        def map_row row

        end
    end

end
