module Conf

    class Source
        require_relative '../database/source_schema'
        require_relative 'file_manager'
        include Conf ## mixin
        @@fields =[ :name, :host, :base_dir ,:decoder,:flow,:file_manager]
        @@fields.each do |f|
            Source.class_eval(Conf.define_accessor(f))
        end
        require_relative '../get/getter'
        def initialize name,flow= nil
            @flow = flow 
            @name = name
            @folders = []
            @schema = ::Database::Schema::Source::GenericSchema.new self
            
            @kgetter = Getter::GenericFlowGetter 
        end

        ## accessor for the schema used by this source
        def schema klass_name = nil
            if klass_name
                @schema = ::Database::Schema::Source::create(klass_name,self) 
            else 
                return @schema
            end
        end

        ## folders to look into for files
        def folders *args
            if args.size == 0
                return @folders
            else
                @folders = @folders +  args
            end
        end
            
        ## Defines a host, and when defined,
        #defines the filemanager on it !
        def host name = nil
            if name
                @host = Conf.host name
                @file_manager = FileManager::create(self) 
            else
                @host
            end
        end

        # instantiate a given decoder and set 
        # some values like fields to filter etc.
        # klass_name must be a string or symbol, with
        # the exact name of the decoder class !
        def decoder klass_name = nil, opts = {}
            if !klass_name
                ## ACCessor method
                return @decoder
            end
            require_relative '../decoder'
            ## setter method
            @decoder = Decoder::create(klass_name,opts)
            # setup the filter if exists
            @decoder.filter = @flow.filter if @flow.filter
            opts.keys.each do |attr|
                next unless @decoder.respond_to?(attr.to_sym)
                @decoder.instance_variable_set("@#{attr}",opts[attr])
            end
        end

        ## Sets the mapper in place, and link it to the decoder if possible 
        def mapper klass_name = nil, opts = {}
            if !klass_name
                return @mapper
            end
            require_relative '../mappers/generic'
            ## no need for a mapper that is the Identity (it exists still)
            return if klass_name =~ /identity/i
            @mapper = Mapper::create(klass_name,opts)
            @decoder.mapper = @mapper if @decoder # link both if exists
        end

        ## list of fields outputed by the decoder then mapped and filtered !
        # i.e. the fields that we expect in the DB 
        def records_fields
            @records_fields ||= @decoder.fields
            return @records_fields
        end

        # simply overrides the default value of the File Manager
        def file_manager_options opts = {}
            opts.keys.each do |attr|
                a = "@#{attr}"
                @file_manager.instance_variable_set(a,opts[attr])
            end
        end


        def ==(other)
            other.name == self.name
        end
        alias :eql? :==
    end

end
