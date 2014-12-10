monitor "sms_stats" do
    sources "sms_in","sms_out"
    time_interval 1.hour
    nat = Proc.new { |n| n.start_with?("07") || n.start_with?("417") }
    stats do 
        list("onnet",:b_imsi,:calling_vmsc_no_number) do |i,vn| 
            i.start_with?("22803") && nat.call(vn) 
        end
        list("offnet",:b_number,:b_imsi) do |n,i|
            (i.start_with?("22803") || i.empty?) && nat.call(n)
        end
        list("int",:b_number) do |n|
            !nat.call(n)
        end
        list("roaming_nat",:b_number,:calling_vmsc_no_number) do |n,vn|
            nat.call(n) && !nat.call(vn)
        end
        list("roaming_int",:b_number,:calling_vmsc_no_number) do |n,vn|
            !nat.call(n) && !nat.call(vn)
        end
        ## shortcode
        list("sc_free_nat",:status,:tariff_class,:calling_vmsc_no_number) do |s,t,vn|
            s == "R" && !t.empty? && nat.call(vn) 
        end
        list("sc_free_roaming",:status,:tariff_class,:calling_vmsc_no_number) do |s,t,vn|
            s == "R" && !t.empty? && !nat.call(vn)
        end
    end
end
monitor "sms_instats" do
    time_interval 1.hour
    sources "sms_in"
    nat = Proc.new { |n| n.start_with?("07") || n.start_with?("417") }
    stats do 
        ## premium
        list("sc_price_nat",:status,:tariff_class,:called_vmsc_no_number) do |s,tc,vn|
            s == "R" && !tc.empty? && nat.call(vn)
        end
        list("sc_price_roaming",:status,:tariff_class,:called_vmsc_no_number) do |s,tc,vn|
            s == "R" && !tc.empty? && !nat.call(vn)
        end
    end
end
