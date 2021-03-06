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
monitor "mss_stats" do
    schema :NewGenericSchema
    ## Numbering plans for national
    fixed = %w(21 22 24 26 27 31 32 33 34 41 43 44 51 52 55 56 58 61 62 71 81 91)
    mobiles = %w(75 76 77 78 79)
    vas = %w(800 840 842 844 848 878 900 901 906)
    specials = %w(860 868 869 98 99 1)
    all = fixed + mobiles + vas + specials 

    prep = Proc.new do |n,&block| ## Preprocessor function, that cuts down the 
        ## useless leading numbers
        if n.start_with?("41") # country code
            block.call n[2..-1]
        elsif n.start_with?("0") # just leading 0
            block.call n[1..-1]
        else
            block.call(n) # national destination code
        end
    end

    national = Proc.new do |ton,number,&block|
        res = false
        if ton == 6
            number = number[2..-1] if number.start_with?("41") && number.length > 10
            res = true & block.call(number)
        elsif ton == 5
            if number.start_with?("41")    
                res = true & block.call(number[2..-1])
            else 
                res = false
            end
        end
        res
    end


    sources "mss_in","mss_out"
    time_interval 1.hour

    filter do 
        field :record_type  
        field :called_number
        field :called_number_ton
    end
    stats do 
        ## TYPES
        list("POC",:record_type) { |x| x.to_i == 11 }
        list("MOC",:record_type) { |x| x.to_i == 1 }
        list("FORW", :record_type) { |x| x.to_i == 3 }

        ## NAT
        list("fixed",:record_type,:called_number,:called_number_ton) do |rec,n,ton|
            rec.to_i == 1 && !ton.empty? && national.call(ton.to_i,n) { |nn|  fixed.any? { |x| nn.start_with?(x) } }
        end
        list("mobile",:record_type,:called_number,:called_number_ton) do |rec,n,ton|
            rec.to_i == 1 && !ton.empty? && national.call(ton.to_i,n) { |nn| mobiles.any? { |x| nn.start_with?(x) } }
        end
        list("vas",:record_type,:called_number,:called_number_ton ) do |rec,n,ton|
            rec.to_i == 1 && !ton.empty? && national.call(ton.to_i,n)  { |nn| vas.any? { |x| nn.start_with?(x) } }
        end
        list("specials",:record_type,:called_number,:called_number_ton) do |rec,n,ton|
            rec.to_i == 1 && !ton.empty? && national.call(ton.to_i,n) { |nn| specials.any? { |x| nn.start_with?(x) } }
        end
        ## INT
        list("int_moc",:record_type,:called_number_ton,:called_number) do |a,b,c|
            a.to_i == 1 && !b.empty? && b.to_i == 5 && !c.start_with?("41")
        end
        list("int_poc",:record_type,:called_number_ton,:called_number) do |a,b,c|
            a.to_i == 11 && !b.empty? && b.to_i == 5 && !c.start_with?("41")
        end
    end
end
