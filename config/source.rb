module Conf

    class Source
        require_relative '../database'
        require_relative 'file_manager'
        include Conf ## mixin
        @@fields =[ :name, :host, :base_dir ,:decoder,:flow,:file_manager,:filters,:file_length]
        @@fields.each do |f|
            Source.class_eval(Conf.define_accessor(f))
        end
        require_relative '../get'
        require_relative '../insert'
        def initialize name,flow= nil
            @flow = flow 
            @name = name
            @folders = [""]
            @schema = ::Database::Schema::Source::GenericSchema.new self
            ## generic one 
            @kgetter = Getter::GenericSourceGetter 
            @kinserter = Inserter::GenericSourceInserter
        end

        ## accessor for the schema used by this source
        def schema klass_name = nil,opts = {}
            if klass_name
                @schema = ::Database::Schema::Source::create(klass_name,self,opts) 
            else 
                return @schema
            end
        end

        ## folders to look into for files
        def folders *args
            if args.size == 0
                return @folders
            else
                ## We are putting an entry in it so delete the "empty default" one
                @folders = [] if @folders.first.empty?
                @folders = @folders +  args
            end
        end
        alias :folder :folders

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

        ## Ugly yes... Workaroud to set the filter 
        #of the decoder . Since we specify the filter after the decoder
        # we need to link thoses two ..
        #
        def apply_filter 
            @decoder.filter = @filter || @flow.filter
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
            # setup the filter of the source if exists or the flow
            @decoder.filter = @filter || @flow.filter
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

        def getter kname
            kname = kname.to_sym
            unless Getter::const_defined?(kname)
                raise "Source #{self.name}: No getter found by this name.Abort"
            end
            @kgetter = Getter::const_get(kname)
        end

        def get opts = {}
            @getter = @kgetter.new(self,opts)
            @getter.get
        end

        def inserter kname, opts = {}
            kname = kname.to_sym
            unless Inserter::const_defined?(kname)
                raise "Source #{self.name}: No Inserter found by this name(#{kname}).Abort"
            end
            @kinserter = Inserter::const_get(kname)
        end

        def insert opts = {}
            @inserter = @kinserter.new(self,opts)
            @inserter.insert
        end

        def ==(other)
            other.name == self.name
        end
        alias :eql? :==


        def set_options opts =  {}
            @file_manager.set_options(opts) if @file_manager
            @schema.set_options(opts) if @schema
        end

    end

end
