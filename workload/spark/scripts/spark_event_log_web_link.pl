#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV != 1) {
    print "Usage: ./spark_event_log_web_link.pl <INFO> <TAG>\n";
    exit 1;
}

my $info_fn = $ARGV[0];
my $tag = $ARGV[1];
my $debug_fn = $info_fn;
$debug_fn =~ s/info$/debug.log/;

my $app_count = `grep "TAG:$tag " $info_fn | grep "APPID" | wc -l`;
chomp($app_count);
if ($app_count == 0) {
    open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
    print DEBUG "spark_event_log_web_link.pl $info_fn $tag\n";
    print DEBUG "Cannot find APPID\n\n";
    close DEBUG;
    print "NA\n";
    exit 1;
}

my $app_id = `grep "TAG:$tag " $info_fn | grep APPID | head -n 1 | awk '{print \$3}' | awk -F: '{print \$2}'`;
chomp($app_id);
my $count = `ps -ef | grep java | grep -i historyserver | grep -v org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer | awk '{print \$2}' | xargs -i sh -c \"netstat -nap | grep {}\" | grep LISTEN | awk '{print \$4}' | awk -F: '{print \$NF}' | wc -l`;
chomp($count);
if ($count == 0) {
    print "NA\n";
    exit 1;
}
my $port = `ps -ef | grep java | grep -i historyserver | grep -v org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer | awk '{print \$2}' | xargs -i sh -c \"netstat -nap | grep {}\" | grep LISTEN | awk '{print \$4}' | awk -F: '{print \$NF}'`;
chomp($port);
if ($? != 0) {
    print "NA\n";
    exit 1;
}
`ip route show | grep ^default | awk '{print \$5}' | xargs -i ifconfig {} | grep netmask > /dev/null 2>&1`;
if ($? != 0) {
    print "NA\n";
    exit 1;
}
my $ip = `ip route show | grep ^default | awk '{print \$5}' | xargs -i ifconfig {} | grep netmask | awk '{print \$2}'`;
chomp($ip);
print "<a href=\"http://".$ip.":".$port."/history/".$app_id."\">".$app_id."</a>\n";
exit 0;
