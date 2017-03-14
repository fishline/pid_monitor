#!/usr/bin/perl
use warnings;
use strict;

if ($#ARGV != 3) {
    print "Usage: ./filter_history.pl <Begin time> <End time> <File includes mr job history> <Type: MAPREDUCE or SPARK>\n";
    exit 1;
}

my $begin_unix_time = `date --date='$ARGV[0]' +"%s"`;
chomp($begin_unix_time);
my $end_unix_time = `date --date='$ARGV[1]' +"%s"`;
chomp($end_unix_time);
my $ret = `grep "href" ./$ARGV[2] | grep "SUCCEEDED" | grep $ARGV[3] | awk -F, '{print \$1 " " \$2 " " \$3}'`;
my @lines = split(/\n/, $ret);
foreach my $line (@lines) {
    if ($line =~ /^\["([^"]+)"\s+"([^"]+)"\s+"([^"]+)"/) {
        my $start = $1;
        my $stop = $2;
        my $job_info = $3;
        
        $start =~ s/\./\//g;
        $stop =~ s/\./\//g;
        my $start_time = `date --date='$start' +"%s"`;
        chomp($start_time);
        my $stop_time = `date --date='$stop' +"%s"`;
        chomp($stop_time);
        if (($start_time > $begin_unix_time) and ($stop_time < $end_unix_time)) {
            if ($job_info =~ />(job_[0-9]+_[0-9]+)</) {
                my $job_id = $1;
                print $job_id."\n";
            }
        }
    }
}
exit 0;
