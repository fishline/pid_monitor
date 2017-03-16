#!/usr/bin/perl
use warnings;
use strict;

if ($#ARGV != 2) {
    print "Usage: ./filter_history.pl <Begin time> <End time> <File includes mr job history>\n";
    exit 1;
}

my $begin_unix_time = `date --date='$ARGV[0]' +"%s"`;
chomp($begin_unix_time);
my $end_unix_time = `date --date='$ARGV[1]' +"%s"`;
chomp($end_unix_time);
my $ret = `grep -A 3 "application_" ./$ARGV[2] | tr '\\n' ' ' | tr '\\-\\-' '\n' | awk -F\\" '{print \$2 " " \$4 " " \$6}' | grep application | awk -F\\/ '{print \$3}'`;
my @lines = split(/\n/, $ret);
foreach my $line (@lines) {
    if ($line =~ /^([^\s]+)\s+([0-9]+)\s+([0-9]+)/) {
        my $job_info = $1;
        my $start = $2 / 1000;
        my $stop = $3 / 1000;
        
        if (($start > $begin_unix_time) and ($stop < $end_unix_time)) {
            print $job_info."\n";
        }
    }
}
exit 0;
