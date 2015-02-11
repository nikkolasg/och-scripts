monitor "sms_stats" do
    schema :NewGenericSchema
    sources "sms_in","sms_out"
    time_interval 1.hour
    free_tc = [100000,199999]
                    ## 07 for input                417 for output emm formatting
    nat = Proc.new { |n| n.start_with?("07") || n.start_with?("417") ||  n.start_with?("423") || n.start_with?("00423") }
    och = Proc.new { |n| n.start_with?("078") }
    ch_imsi = Proc.new { |n| !n.empty? && n.start_with?("22803") }
    stats do 
        ## MOBILE ORIGINATED
        list("mo_nat_onnet",:status,:b_imsi,:calling_vmsc_no_number) do |s,i,vn| 
            s == "S" && nat.call(vn) && !i.empty? && i.start_with?("22803") 
        end
        list("mo_nat_offnet",:status,:b_number,:b_imsi,:calling_vmsc_no_number) do |s,n,imsi,vn|
            s == "S" && nat.call(vn) && (imsi.empty? || !imsi.start_with?("22803")) && n.length > 6
        end
        list("mo_nat_int",:status,:b_number,:calling_vmsc_no_number) do |s,n,vn|
            s == "S" && nat.call(vn) && n.start_with?("00") 
        end
        list("mo_nat_short",:status,:b_number,:calling_vmsc_no_number) do |s,n,vn|
            s == "S" && nat.call(vn) && n.length <= 6
        end  
        
        list("mo_roam_onnet",:status,:b_imsi,:a_imsi,:calling_vmsc_no_number,:a_pid) do |s,bimsi,aimsi,vn,pid|
            res = s == "S" && !nat.call(vn) && ch_imsi.call(aimsi) && ch_imsi.call(bimsi) && pid == "plmn"
            res
        end
        list("mo_roam_offnet",:status,:b_number,:b_imsi,:a_imsi,:calling_vmsc_no_number,:a_pid) do |s,n,bimsi,aimsi,vn,pid|
            s == "S" && !nat.call(vn) && !ch_imsi.call(bimsi) && !ch_imsi.call(bimsi) && n.length > 6 && pid == "plmn"
        end
        list("mo_roam_int",:status,:b_number,:calling_vmsc_no_number,:a_pid) do |s,n,vn,pid|
            s == "S" && !nat.call(vn)  && n.start_with?("00") && pid == "plmn"
        end
        list("mo_roam_short",:status,:b_number,:calling_vmsc_no_number,:a_pid) do |s,n,vn,pid|
           s == "S" && !nat.call(vn) && n.length <= 6 && pid == "plmn"
        end 
        list("mo_roam_total",:status,:calling_vmsc_no_number,:a_pid) do |s,vn,pid|
           s == "S" && !nat.call(vn) && pid == "plmn"
        end 
        
        ##MOBILE TERMINATED
        list("mt_nat_m2m",:status,:tariff_class,:called_vmsc_no_number) do |s,tc,vn|
            s == "R" && nat.call(vn) && tc.empty? 
        end
        list("mt_nat_free",:status,:tariff_class,:called_vmsc_no_number) do |s,tc,vn|
            s == "R" && nat.call(vn) && !tc.empty? && (tc == "100000" || tc == "199999") 
        end
        list("mt_nat_mb",:status,:tariff_class,:called_vmsc_no_number) do |s,tc,vn|
            s == "R" && nat.call(vn) && !tc.empty? && tc != "100000" && tc != "199999" 
        end
        list("mt_roam_m2m",:status,:tariff_class,:called_vmsc_no_number) do |s,tc,vn|
            s == "R" && !nat.call(vn) && tc.empty?
        end
        list("mt_roam_free",:status,:tariff_class,:called_vmsc_no_number) do |s,tc,vn|
            s == "R" && !nat.call(vn) && (tc == "100000" || tc == "199999") 
        end
        list("mt_roam_mb",:status,:tariff_class,:called_vmsc_no_number) do |s,tc,vn|
            s == "R" && !nat.call(vn) && tc != "100000" && tc != "199999"
        end
        list("mt_roam_total",:status,:called_vmsc_no_number) do |s,vn|
            s == "R" && !nat.call(vn)      
        end

        ## shortcode
        #list("sc_free_nat",:status,:tariff_class,:calling_vmsc_no_number) do |s,t,vn|
            #s == "R" && !t.empty? && nat.call(vn) 
        #end
        #list("sc_free_roaming",:status,:tariff_class,:calling_vmsc_no_number) do |s,t,vn|
            #s == "R" && !t.empty? && !nat.call(vn)
        #end
    end
end
