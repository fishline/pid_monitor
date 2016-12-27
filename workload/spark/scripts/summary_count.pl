#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV != 1) {
    print "Usage: ./summary_count.pl <TAG> <INFO_FN>\n";
    exit 1;
}

my $tag = $ARGV[0];
my $info_fn = $ARGV[1];
my $debug_fn = $info_fn;
$debug_fn =~ s/info$/debug.log/;

my $success_count = `grep "TAG:$tag " $info_fn | grep "STATUS:0" | wc -l`;
my $fail_count = `grep "TAG:$tag " $info_fn | grep "STATUS:1" | wc -l`;
my $total_count = `grep "TAG:$tag " $info_fn | grep "COUNT" | awk '{print \$2}' | awk -F: '{print \$2}'`;
if (($success_count + $fail_count) == 0) {
    # Maybe spark standalone scheduler
    my $line = `grep "TAG:$tag " $info_fn | grep EVENTLOG | awk '{print \$3}' | awk -F: '{print \$2}'`;
    my @lines = split(/\n/, $line);
    foreach my $l (@lines) {
        chomp($l);
        my $duration = `$ENV{'PMH'}/workload/spark/scripts/analyze_spark_event_log.sh $l`;
        if ($? != 0) {
            $fail_count = $fail_count + 1;
        } else {
            $success_count = $success_count + 1;
        }
    }
}

if ($total_count == ($success_count + $fail_count)) {
    print "$success_count/$fail_count\n";
} else {
    open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
    print DEBUG "summary_count.pl $tag $info_fn\n";
    print DEBUG "total_count:$total_count does not equal success_count:$success_count + fail_count:$fail_count\n\n";
    close DEBUG;
    print "NA\n";
    exit 1;
}

exit 0;
