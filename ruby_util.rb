module RubyUtil

    # Upper limit on number of elements
    # to when to partition a task
    # collection must respond to slice
    CHUNK_SIZE = 100
    def self.partition collection
        return unless block_given?
        # No need for partitionning
        if collection.size <= CHUNK_SIZE
            yield collection
            return
        end

        counter = collection.size / CHUNK_SIZE
        rest = collection.size % CHUNK_SIZE
        # yield for each "slice"
        counter.times do |n|
            low = n * CHUNK_SIZE
            sub = collection.slice(low,CHUNK_SIZE) 
            yield sub
        end
        unless rest == 0
            # yield for the rest
            sub = collection.slice( counter*CHUNK_SIZE, (counter*CHUNK_SIZE) + rest)
            yield sub
        end
    end

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
    def self.sqlize list,opts = {}
        str = opts[:no_quote] ? list : RubyUtil::quotify(list)
        opts[:no_parenthesis] ? str.join(',') : "(" + str.join(',') + ")"
    end

    def self.require_folder folder
        Dir["#{folder}/*.rb"].each { |f| require "./#{f}" }
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

