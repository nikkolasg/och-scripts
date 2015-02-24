#!/usr/bin/perl
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