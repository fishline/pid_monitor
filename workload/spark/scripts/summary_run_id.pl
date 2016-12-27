#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV != 0) {
    print "Usage: ./summary_run_id.pl <TAG>\n";
    exit 1;
}

my $tag = $ARGV[0];
print "$tag\n";
