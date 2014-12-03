monitor "mms_instats" do
    sources "mms_in"
    time_interval 1.hour

    ch = "mms.orangemail.ch"
    ch2 = "mms.mnc003.mcc228.gprs"
    fl = "mms.orangemail.fl"
    fl2 = "mms.mnc002.mcc295.gprs"
    swisscom = "mms.mnc001.mcc228.gprs"
    sunrise = "mms.mnc002.mcc228.gprs"

    all = [ch,ch2,fl,fl2,swisscom,sunrise]
    stats do
        #################
        ## RECEIVE stats
        ##################
        ## A NUMBER STATS
        list("r_sender_och",:action,:a_mail) { |a,x| a == "R" && x == ch }
        list("r_sender_fl",:action,:a_mail) { |a,x| a == "R" && x == fl }
        list("r_sender_sunrise",:action,:a_mail) { |a,x| a == "R" && x == sunrise }
        list("r_sender_swisscom",:action,:a_mail) { |a,x| a == "R" && x== swisscom }
        list("r_sender_others",:action,:a_mail) {|a,x| a == "R" && !all.include?(x)}
        ## BNUMBER STATS
        list("r_rec_och",:action,:b_mail) {|a,x| a == "R" && (x == ch || x == ch2) }
        list("r_rec_fl",:action,:b_mail) { |a,x| a == "R" && (x == fl || x == fl2) }
        ## postmaster
        list("r_rec_post",:action,:b_mail) { |a,x| a == "R" && x =~ /PostMaster/i }
        #shord code = sc
        list("r_rec_shortcode",:action,:b_mail) { |a,x| a == "R" &&     
                                                  x == '' }
        ## OTHERS ??i
        # b_mail can be mm.mnc003.mcc228.gprs / 002-295 alos !

        ##############
        # SEND STATS
        # ############
        list("s_rec_och",:action,:b_mail) {|a,x| a == "S" && x == ch }
        list("s_rec_fl",:action,:b_mail) { |a,x| a == "S" && x == fl }
        list("s_rec_swisscom",:action,:b_mail) { |a,x| a == "S" && x == swisscom }
        list("s_rec_sunrise",:action,:b_mail) { |a,x| a == "S" && x == sunrise }
        list("s_rec_mail",:action,:b_mail) { |a,x| a == "S" && !all.include?(x) }
        list("s_rec_others",:action,:b_mail) { |a,x| a == "S" && x == '' }


    end
end
#################
## GENERAL STATS
#################
monitor "mms_stats" do
    sources "mms_in","mms_out"
    time_interval 1.hour

    stats do
        list("send",:action) { |x| x == "S" }
    end
end


