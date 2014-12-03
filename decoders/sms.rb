module Decoder

    class LogicaDecoder
        include Decoder
        
        # print with column name before
        COMMAND = "decoding/logica.pl -dc "

        def initialize(opts = {})
            @opts = opts
        end

        def decode file
            check file
            cmd = COMMAND + file.full_path
            out = exec_cmd(cmd)

        end

    end

end
