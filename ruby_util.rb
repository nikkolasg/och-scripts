#
# Copyright (C) 2014-2015 Nicolas GAILLY for Orange Communications SA, Switzerland
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
module RubyUtil
      

    def self.max coll
        coll.inject(0) { |col,value| value > col ? value : col }
    end

    ## Find the common subset of all sets / array given
    def self.commom_subset *lists
        return [] if lists.size == 0
        return lists.first if lists.size == 1
        sub = lists.shift
        lists.each do |list|
            sub = sub & list
        end
    end

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
    
    ## Can symbolize keys of a Hash, or every elements of an Array
    # Can apply a preprocessing step on the element with :send opts,
    # it will call the method on each element
    def self.symbolize coll,opts = {}
        if coll.is_a?(Hash)
            return coll.inject({}) do |new,(k,v)|
            nk = k
            nk = k.send(opts[:send]) if opts[:send]
            nk = nk.to_sym
            if v.is_a?(Hash)
                new[nk] = RubyUtil::symbolize(v)
            else
                new[nk] = v
                new[nk] = v.to_sym if opts[:values]
            end
            new
        end 
        elsif coll.is_a?(Array)
            return coll.map { |c| opts[:send] ? c.send(opts[:send]).to_sym : c.to_sym }
        elsif coll.is_a?(String)
            return coll.to_sym
        end
    end
    def self.arrayize value
        if value.is_a?(Array)
            value
        else
            [ value ]
        end
    end
    def self.escape value
        esc = lambda {|x| x.gsub(/\\|'/) { |c| "\\#{c}" } }
        if value.is_a?(String)
            esc.call(value)
        elsif value.is_a?(Array)
            value.map { |v| self.escape(v) }
        else
            value
        end
    end
    def self.quotify list
        list.map { |l| "'#{self.escape(l)}'"}
    end
    def self.sqlize list,opts = {}
        str = opts[:no_quote] ? list : RubyUtil::quotify(list)
        opts[:no_parenthesis] ? str.join(',') : "(" + str.join(',') + ")"
    end

    def self.require_folder folder
        Dir["#{folder}/*.rb"].each { |f| require_relative "#{f}" }
    end


end
# must be outside of the module
# if not, different namespace will be created
class Fixnum
    MIN_IN_HOURS = 60
    HOURS_IN_DAY = 24
    ## Approximatively
    DAYS_IN_MONTH = 30
    def months
        days * DAYS_IN_MONTH
    end
    alias :month :months

    def days
        hours * HOURS_IN_DAY
    end
    alias :day :days

    def hours
        minutes * MIN_IN_HOURS
    end
    alias :hour :hours

    def minutes
        self
    end
    alias :minute :minutes
end

require 'singleton'
class SignalHandler
    include Singleton

    @@count = 0
    MAX_TRY = 5
    def initialize 
        @breaker = false
        @blocks = []
    end

    def self.ensure_block &block
        @blocks = [] unless @blocks
        @blocks << block
    end

    def self.enable
        trap("INT") do
            @breaker = true
            yield if block_given?
            exit if @debug
            exit if @@count > MAX_TRY
            @@count += 1
        end
    end

    def self.debug
        @debug = true
    end

    def self.check
        if @breaker
            yield if block_given?
            @blocks.each { |b| b.call }
            exit
        end
    end
end
