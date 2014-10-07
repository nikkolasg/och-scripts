module RubyUtil

    def self.symbolize hash
        hash.inject({}) do |new,(k,v)|
            if v.is_a?(Hash)
                new[k.to_sym] = RubyUtil::symbolize(v)
            else
                new[k.to_sym] = v
            end
            new
        end
    end
    def self.arrayize value
        if value.is_a?(Array)
            value
        else
            [ value ]
        end
    end

end
