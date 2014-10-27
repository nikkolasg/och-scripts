#!/usr/bin/perl
use POSIX 'strftime';
use Net::SMTP; # this is needed for the e-mail functionalities

# use these function by adding this in your perl script 'require "alert.pl";'
# this "module" provides the standard function that are needed for 
# alerting script events and alarms
# version 0.8.2, 2004-09-24 jd (base/working version)
# version 0.9.0, 2010-05-18 jd (adapted to config.cfg)
# version 0.9.1, 2010-07-07 jd (actually made it working)
# version 0.9.2, 2010-07-28 jd (print onto terminal as well, if available)
# version 0.9.3, 2011-07-25 jd (fixed replay address setting for mailx -a instead of -r)
# version 0.9.4, 2011-07-26 jd (added "autoflush" to LOGFILE writting
# version 0.9.5, 2011-10-17 jd (using sms_kannel client)

# by Prepaid Services - jan.diener-rodriguez@orange.ch
# ==============================================================================

# directory path, etc - must be set in the actual script, typically like below
# ----------------------------------------------------------------------------
# $0 =~ /(^.*\/)(.+)\.(.*)$/ ;
# $SCRIPTNAME=$2;
$APPLICATION= defined $PARAM{'APPLICATION_NAME'} ? $PARAM{'APPLICATION_NAME'} : $EXECUTIONDIR ;

# Data relevant for e-mail stuff, better if set in the actual script
# ----------------------------------------------------------------------------
# $MAILSERVER='172.19.53.6'; # to use atlas as a gateway
$MAILSERVER  = defined $PARAM{'MAILSERVER'} ? $PARAM{'MAILSERVER'} : "local" ; # to use the local mail client
$REPLYADDRESS= defined $PARAM{'REPLYADDRESS'} ? $PARAM{'REPLYADDRESS'} : 'prepaid.services@orange.ch' ;
@EMAIL_ALARM = defined $PARAM{'EMAIL_ALARM'} ? split(",",$PARAM{'EMAIL_ALARM'})
                                             : ('jan.diener-rodriguez@orange.ch');
@SMS_ALARM = defined $PARAM{'SMS_ALARM'} ? split(",",$PARAM{'SMS_ALARM'})
                                             : ('0787872747');                                             
$FOOTER = defined $PARAM{'EMAIL_FOOTER'} ? $PARAM{'EMAIL_FOOTER'}
                                         : 'powered by prepaid.services@orange.ch';
                                         
$PARAM{'LOGLEVELS'} = "1:DEBUG,2:INFO,3:WARNING,4:ERROR,5:FATAL" unless defined $PARAM{'LOGLEVELS'};
%loglevels = ();
foreach my $levelentry (split(",",$PARAM{'LOGLEVELS'})) {
  my($id,$txt) = split(":",$levelentry);
  $loglevels{$id} = $txt; 
}
%loglevels_rev = reverse %loglevels;
$level_log   = defined $PARAM{'LEVEL_LOG'}   ? $PARAM{'LEVEL_LOG'}   : 3 ;
$level_email = defined $PARAM{'LEVEL_EMAIL'} ? $PARAM{'LEVEL_EMAIL'} : 5 ;
$level_sms   = defined $PARAM{'LEVEL_SMS'}   ? $PARAM{'LEVEL_SMS'}   : 5 ;


# -----------------------------------------------------------------------------
# "enable" auto flush for LOGFILE
# http://perl.plover.com/FAQs/Buffering.html
# =============================================================================
select((select(LOGFILE), $|=1)[0]);


# -----------------------------------------------------------------------------
# BASIIC sub for script checking & logging
# =============================================================================



sub write_die {
  my $msg       = shift;
  my $level     = shift;
  my $flag      = shift; # ="LOG" will die but not alert by e-mail

  # not handing over $flag, as write_log is only supposed to write to LOGFILE
  &write_log($msg . " => Processing has been stopped", $level, "LOG");
  close LOGFILE;
  $msg .= "\nProcessing has been stopped";

  if ( $flag !~ /LOG/ ) {
    &send_email(\@EMAIL_ALARM,log_date()." (pid:$$) $msg","STOPPED") unless ($flag =~ /LOG/) ;
    &send_sms(\@SMS_ALARM,$msg,"STOPPED");
  }
  die log_date()." (pid:$$) $msg";
}

sub write_log {
  # flags: "ALERT" will force EMAIL&SMS;
  # "EMAIL" force e-mail;
  # "SMS" force sms;
  # "LOG" will not send EMAIL nor SMS
  
  my $msg   = shift;
  my $level = shift;
  my $flag  = shift; # ="ALERT" will send e-mail

  if (defined $level) {
    # if $loglevels{$level} is defined we are happy
    if (!defined($loglevels{$level})) {
      $level = defined($loglevels_rev{$level}) ? $loglevels_rev{$level} : 4 ;
    }
  }
  else {
    $level = 4;
  }
  
  print log_date() . "\t$level\tprinting to LOGFILE \n" if $level_log <= 1;
  if ($level >= $level_log) {
    # print to stdout if terminal..
    print log_date()." " . $loglevels{$level}. "\t(pid:$$) $msg\n" if ( -t STDOUT );
    print LOGFILE log_date()." " . $loglevels{$level}. "\t(pid:$$) $msg\n";
  }

  $msg .= "\n\nProcessing has NOT been stopped\nbut you might want to check during the day ;-)\n";
  if ($flag =~ /ALERT/i || $flag =~ /EMAIL/i || ($level >= $level_email && $flag !~ /LOG/) ) {
    &send_email(\@EMAIL_ALARM,log_date()." (pid:$$) $msg",$loglevels{$level})  ;
  }
  if ($flag =~ /ALERT/i || $flag =~ /SMS/i || ($level >= $level_sms && $flag !~ /LOG/) ) {
    &send_sms(\@SMS_ALARM,$msg,$loglevels{$level});
  }
}

sub log_date {
  $datestring = strftime "%Y-%m-%d %H:%M:%S", localtime;
  return $datestring;
}

sub send_sms {
  my $address = shift;
  my $msg     = shift;
  my $subject = shift;
  my $file    = shift;

  $subject=$APPLICATION.' - '.$SCRIPTNAME.' - '.$subject;
  #$subject = $SCRIPTNAME.' - '.$subject;

  $sms_bin = "/home/mlog/tool/send_sms_kannel.pl";
  if ( -x $sms_bin  ) {  # check if sms_send.pl is
    $sms_numbers = join(' ',@{$address});
    $msg = substr($subject . "\n" . $msg,0,140);
    #print "msg: $msg \n" ;
    #print "$sms_bin $sms_numbers\n";
    qx(echo "$msg" | $sms_bin $sms_numbers);
    print "failed to send SMS\n" if ($? != 0);
  }
  else {
    print "ERROR - sms not sent, cannot find $sms_bin\n";
  }
}


sub send_email {
  my $address = shift;
  my $msg     = shift;
  my $subject = shift;
  my $file    = shift;

  $subject=$APPLICATION.' - '.$SCRIPTNAME.' - '.$subject;

  if ($MAILSERVER =~ /local/ ) {
    $email_addr=join(' ',@{$address});
    $fullmsg=$msg."\n".$FOOTER;
    #print "echo '$fullmsg' | mail -s \"$subject\" -r $REPLYADDRESS $email_addr ";
    #system("echo '$fullmsg' | mail -s \"$subject\" -a $file -r $REPLYADDRESS $email_addr ");
    #system("echo '$fullmsg' | mailx -s '$subject' -r $REPLYADDRESS $email_addr ");
    system("echo '$fullmsg' | mailx -s '$subject' -a 'Reply-To: $REPLYADDRESS' $email_addr ");
  }
  else { # by default use altas (or the defined $MAILSERVER)
    # open ATTACHMENT, $file || &write_log("cannot open (read) file $file");
    print "attachment handling yet to be implemented !!" if length($file) > 1;
    # print "MAILSERVER: $MAILSERVER\nto: $address\nSubject: $subject\nmsg: $msg";
    $smtp = Net::SMTP->new($MAILSERVER);
    # $smtp = Net::SMTP->new($MAILSERVER, Debug => 1); # if the e-mails are not sent
  
    $smtp->mail($REPLYADDRESS);
    $smtp->to(@{$address});

    $smtp->data();
    $smtp->datasend("Subject: $subject\n");
    $smtp->datasend("\n");
    $smtp->datasend($msg."\n");
    $smtp->datasend("\n---\n".$FOOTER);
    $smtp->dataend();

    $smtp->quit;
  }
}



# require should return "1"
1
