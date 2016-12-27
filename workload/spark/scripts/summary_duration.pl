#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV != 1) {
    print "Usage: ./summary_count.pl <TAG> <INFO>\n";
    exit 1;
}

my $tag = $ARGV[0];
my $info_fn = $ARGV[1];
my $debug_fn = $info_fn;
$debug_fn =~ s/info$/debug.log/;

my $count = `grep "TAG:$tag " $info_fn | grep "DURATION:" | wc -l`;
chomp($count);
if ($count == 0) {
    print "NA\n";
    exit 1;
}
my $duration_str = `grep "TAG:$tag " $info_fn | grep "DURATION:" | awk '{print \$3}' | awk -F: '{print \$2}'`;
my @durations = split(/\n/, $duration_str);
my $total = 0.0;
foreach my $l (@durations) {
    chomp($l);
    $total = $total + $l;
}

my $avg = $total / $count;
print "$avg\n";
exit 0;
