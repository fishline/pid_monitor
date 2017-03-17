#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV != 1) {
    print "Usage: ./mapreduce_median_sec.pl <INFO_FILE> <TAG>\n";
    exit 1;
}

my $info_fn = $ARGV[0];
my $tag = $ARGV[1];
my $debug_fn = $info_fn;
$debug_fn =~ s/info$/debug.log/;
my $line = `grep "TAG:$tag " $info_fn | grep DURATION`;
my @lines = split(/\n/, $line);
my @durations = ();
foreach my $l (@lines) {
    chomp($l);
    if ($l =~ /\s+ITER:[0-9]+\s+DURATION:([0-9]+)/) {
        my $duration = $1;
        chomp($duration);
        push @durations, $duration;
    }
}
if ($#durations == -1) {
    open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
    print DEBUG "mapreduce_median_sec.pl $info_fn $tag\n";
    print DEBUG "Did not get DURATION from info file\n\n";
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
