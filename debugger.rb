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
module Debug
    module_function
    def debug_filter_fields_allowed h
        h.each do |f,v|
            puts "Field #{f} ==> #{v.inspect}"
        end
        STDIN.gets
    end

    def debug_row fields, row
        h = Hash[fields.zip(row)]
        puts "Fields size : #{fields.size}\tRow size : #{row.size}"
        h.each do |k,v|; puts k.inspect + " => " + v.inspect; end;
        STDIN.gets
    end
    def debug_json json
        json.each do |type,hash|
            puts type
            nh = Hash.new { |h,k| h[k] = [] }
            h = hash[:fields].inject(nh) do |col,(f,i)|
                col["#{f} , #{i}"] = hash[:values].inject([]) { |coll,row| coll << row[i] ; coll }.take(10)
                col
            end
            h.each { |k,v| puts "#{k} ==> #{v}" ;}
            STDIN.gets
        end
    end
    def debug_fields before, after
        before.each do |f,i|
            v = after[f]
            danger = i == v ? "" : "DANGER"
            puts"#{f} [#{i}] ==> #{v} \t\t#{danger}"
        end
        STDIN.gets

    end

    def debug_fields_and_record fields,record
        puts fields.inspect
        puts record.each_with_index.to_a.inspect
        STDIN.gets
    end
end