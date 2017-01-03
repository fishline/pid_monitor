#!/usr/bin/perl
use warnings;
use strict;

if ($#ARGV != 0) {
    print "Usage: do_release_vcore.pl <CONTAINER_ID>\n";
    exit 1;
}

my $id = $ARGV[0];
if (-e "/tmp/vcore.log") {
    my @lines = ();
    open FD, "< /tmp/vcore.log" or die "Cannot open /tmp/vcore.log for read!";
    while (<FD>) {
        my $line = $_;
        if (not ($line =~ /CONTAINER:$id\s+/)) {
            push @lines, $line;
        }
    }
    close FD;
    if ($#lines >= 0) {
        open FD, "> /tmp/vcore.log" or die "Cannot open /tmp/vcore.log for write!";
        foreach my $line (@lines) {
            print FD $line;
        }
        close FD;
    } else {
        `rm -f /tmp/vcore.log`;
    }
}
