#!/usr/bin/perl
# a simple "wrapper" to use the alert.pl "library"
# <script> log_level log_message

# prepaid.service@orange.ch
# v 0.1.0 - 2012-11-15 jd : a frist shot

$loglevel = shift(@ARGV);
$logmsg   = join(" ",@ARGV);

$0 =~ /(^.*\/)(.+)\.(.*)$/ ;
$EXECUTIONDIR = $1;
$SCRIPTNAME = $2;
$CONFIGFILE = "$EXECUTIONDIR/config.cfg";
%PARAM = ();
read_config($CONFIGFILE,\%PARAM);

#$BASEDIR    = $PARAM{BASEDIR};
$LOGFILE    = $PARAM{LOG_DIR}."/".$SCRIPTNAME.".log";
#$LOGFILE    = $PARAM{BASEDIR}."/".$LOGFILE unless ( $LOGFILE =~ /^\// );
$LOGFILE    = $EXECUTIONDIR."/".$LOGFILE unless ( $LOGFILE =~ /^\// );
open (LOGFILE, ">>$LOGFILE") || die(localtime()." cannot open $LOGFILE");

require "$EXECUTIONDIR/alert.pl";

#write_log("aggregator started","INFO");
write_log($logmsg,$loglevel);

# == HELPER ==
# =====================================
sub read_config {
  my $CONFIGFILE = shift;
  my $param_ref = shift;

  open (CONFIG, $CONFIGFILE) || die("cannot open config file: $CONFIGFILE");
  foreach (<CONFIG>) {
    next if $_ =~ /^#/ ;
    $_ =~ /^(.+?)[\s=](.*?)$/; # only use the first "=" or "space" as a separator
    my $name = $1;
    my $value = $2;
    if ( $value =~ /^"(.*)"$/ ) { $value = $1;}
    $param_ref->{"$name"} = $value;
  }
  close (CONFIG);
}
