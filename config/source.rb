module App

    class Source
        require './decoder'
        require './file_manager'
        @@fields =[ :name,:direction, :host, :base_dir ,:decoder,:flow,:file_manager ]
        @@fields.each do |f|
            Source.class_eval(App.define_accessor(f))
        end

        def initialize flow= nil
            @flow = flow 
        end
        def switch(*args)
            if !instance_variable_defined?(:@switches)
                Source.class_eval(App.define_accessor("switches"))
                @switches = []
            end
            @switches = @switches +  args
        end
        def host name = nil
            if name
                @host = App.host name
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
            ## setter method
            @decoder = Decoder::create(klass_name,@flow.records_allowed,
                                       @flow.records_fields.keys)
            opts.keys.each do |attr|
                next unless @decoder.respond_to?(attr.to_sym)
                @decoder.instance_variable_set("@#{attr}",opts[attr])
            end
        end


        # simply overrides the default value of the File Manager
        def options opts = {}
            @file_manager = FileManager::create(self) 
            opts.keys.each do |attr|
                a = "@#{attr}"
                @file_manager.instance_variable_set(a,opts[attr])
            end
        end
    end

end
