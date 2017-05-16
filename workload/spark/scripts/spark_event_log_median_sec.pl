#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV != 1) {
    print "Usage: ./spark_event_log_median_sec.pl <INFO_FILE> <TAG>\n";
    exit 1;
}

my $info_fn = $ARGV[0];
my $tag = $ARGV[1];
my $debug_fn = $info_fn;
$debug_fn =~ s/info$/debug.log/;
`grep "TAG:$tag " $info_fn | grep "STATUS:0" > /dev/null 2>&1`;
if ($? != 0) {
    print "0\n";
    exit 0;
}
my $line = `grep "TAG:$tag " $info_fn | grep "STATUS:0"`;
my @lines = split(/\n/, $line);
my @durations = ();
foreach my $l (@lines) {
    chomp($l);
    if ($l =~ /\s+ITER:([0-9]+)\s+/) {
        my $iter = $1;
        $line = `grep "TAG:$tag ITER:$iter EVENTLOG:" $info_fn`;
        if ($line =~ /\s+EVENTLOG:(.*)$/) {
            my $event_log = $1;
            my $duration = `$ENV{'PMH'}/workload/spark/scripts/analyze_spark_event_log.sh $event_log`;
            if ($? != 0) {
                open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
                print DEBUG "spark_event_log_median_sec.pl $info_fn $tag\n";
                print DEBUG "Got spark eventlog failed, not found\n\n";
                close DEBUG;
            } else {
                chomp($duration);
                push @durations, $duration;
            }
        }
    }
}
if ($#durations == -1) {
    open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
    print DEBUG "spark_event_log_median_sec.pl $info_fn $tag\n";
    print DEBUG "Got spark eventlog failed, but yarn application -list shows finished\n\n";
    close DEBUG;
    print "NA\n";
} else {
    my $idx = 0;
    my @sorted_durations = sort @durations;
    if (($#sorted_durations + 1) % 2 == 1) {
        $idx = (($#sorted_durations + 2) / 2) - 1;
        print $sorted_durations[$idx]."\n";
    } else {
        $idx = (($#sorted_durations + 1) / 2) - 1;
        my $result = ($sorted_durations[$idx] + $sorted_durations[$idx + 1]) / 2.0;
        $result = sprintf("%.1f", $result);
        print $result."\n";
    }
}
