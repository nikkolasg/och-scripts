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
    def self.quotify list
        list.map { |l| "'#{l}'"}
    end
    def self.sqlize list
        "(" + RubyUtil::quotify(list).join(',') + ")"
    end

end
# must be outside of the module
# if not, different namespace will be created
class Fixnum
    MIN_IN_HOURS = 60
    def hours
        self * MIN_IN_HOURS
    end
    alias :hour :hours

    def minutes
        self
    end
    alias :minute :minutes
end

