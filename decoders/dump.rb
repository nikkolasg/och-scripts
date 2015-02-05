## Module that can dump mysql statements to describe a log file
# It can take file name as input, with the right decoder (in decoders/ folder)
# or CDR::File objects ..
# It will make an average and decide on best situation 
# can write the dump in a file specified
module MysqlDumper
    require_relative '../cdr'
    require_relative '../mappers/generic'
    MAX_DEV = 2
    module_function


    # file must be in format
    # column_name:SQL TYPE
    # output format is a Hash
    # key => column_name
    # value => sql type
    def read_dump_file file
        hash = {}
        File.read(file).split("\n").each do |line|
            field,sql = line.split ':'
            hash[field.downcase.to_sym] = sql
        end
        hash
    end

    def dump decoder, files, opts = {}
        list = transform files
        info,fields,names = stats decoder,list
        sql = {}
        info.each do |field,values|
            shared = is_shared? names,fields[field]
            v = analyze shared,field,values
            sql[field] = v if v
        end
        write_to_file sql,opts[:file]  if opts[:file]
        sql
    end

    def write_to_file sql,file
        open(file,"w+") do |f|
            sql.each do |field,sql_|
                f.puts "#{field}:#{sql_}"
            end
        end
    end

    # main function that will analyze 
    # the values and return SQL statement
    # if this field is shared amongst every records or not
    # and the values for this field
    def analyze shared,field,values
        if field.to_s.start_with?(Util::TIME_PREFIX) ## TIME STAMP !
            v = "INT UNSIGNED DEFAULT 0"
            Logger.<<(__FILE__,"INFO","DUMP FIELD : #{field} => #{v}")
            return v
        end 
        ## analyze otherwise
        size_values = values.map { |c| c.size }
        mean = mean(size_values)
        max = max(size_values)
        dev = std_dev(size_values,mean)
        ## check number, letters 
        num,alpha,hexa = detect_input values
        if max == 0
            print "Field #{field} has a max of 0, do you want to keep it ? (y/n) : "
            v = ""
            v = STDIN.gets.chomp while v != "y" && v != "n"
            if v == "y" 
                v = ""
                print "Enter the MySql type you want for this field : "
                v = STDIN.gets.chomp while v.empty?
                return "VARCHAR(#{v.to_i}) DEFAULT ''"
            else
                return false
            end
        end
        ## for now, only consider CHAR or VARCHAR values
        # due to the casting pb in the the monitoring tool
        if shared && dev <= MysqlDumper::MAX_DEV
            v = "CHAR(#{max}) DEFAULT ''"
        elsif mean < 2 ## if size < 2, no need to varchar, otehrwise making overhead over nothing !
            v = "CHAR(#{max}) DEFAULT ''"
        else
            v = "VARCHAR(#{max}) DEFAULT ''" 
        end
        Logger.<<(__FILE__,"INFO","DUMP FIELD : #{field} => max: #{max} , dev : #{dev}, mean: #{mean} ==> #{v}")
        return v
    end

    def is_shared? names , field_by_names
        names == field_by_names
    end


    ## detect ahh the characteres inside
    def detect_input values
        chars = Set.new
        values.each do |row|
            cs = row.split("")
            chars += cs
        end
        letters = ("a".."z").to_a
        nums = ("1".."9").to_a

        num = (chars & nums).empty? ? false : true
        alpha = (chars & letters).empty? ? false : true
        hexa = (alpha && num) ? (chars.any?{|c| c >= "g"} ? false : true ) : false
        return num,alpha,hexa
    end
    ## transform in CDR::File objects
    def transform files
        files.map do |f|
            v = nil
            if !f.is_a?(CDR::File)
                v = CDR::File.new(f,search: true)
                v.unzip! if v.zip?
            else
                v = f
            end
            v
        end
    end

    ## return a hash with
    # key = field name
    # value = Array of values
    # Array of all fields
    # set of records name
    def stats decoder,file_list
        stats = Hash.new {|h,k| h[k] = [] }
        # or just some particular ones
        # key : field name
        # value : set of records name that share that field
        afields = Hash.new { |h,k| h[k] = Set.new }
        ## collection of records name
        names = Set.new
        file_list.each do |file|
            json = decoder.decode file
            hn = Hash.new { |h,k| h[k] = [] }
            ## s is a hash with
            #key : field name  => arr of values !
            s = json.inject(hn) do |col,(name,hash)|
                names.add(name)
                fields = hash[:fields]
                values = hash[:values]
                ## iteratoe over each field
                fields.each do |field, index|
                    afields[field] << name
                    values.each do |row|
                        col[field] << row[index]
                    end
                end
                col
            end
            s.each { |f,arr|  stats[f] += arr }
        end
        return stats,afields,names
    end

    def max values
        values.inject(0) { |col,i| i > col ? i : col }
    end

    def sum values
        values.inject(0) { |col,i| col += i }
    end

    def mean values
        sum(values) / values.length.to_f
    end

    def std_dev values,mean_ = nil
        m = mean_ || mean(values)
        sum = values.inject(0) { |col,i| col += (i-m)**2 }
        v = Math::sqrt(sum/(values.length - 1).to_f)
        return v
    end

end
