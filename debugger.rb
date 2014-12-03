module Debug
    module_function
    def debug_filter_fields_allowed h
        h.each do |f,v|
            puts "Field #{f} ==> #{v.class}"
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
                    col["#{f} , #{i}"] = hash[:values].inject([]) { |col,row| col << row[i] ; col }.take(10)
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


end
