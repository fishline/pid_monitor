#!/usr/bin/perl
use warnings;
use strict;

my %cpu_rsrc = ();
my $cpus = `numactl -H | grep cpus | awk -F": " '{print \$2}' | tr ' ' '\\n' | sort -n`;
my @cpu_array = split(/\n/, $cpus);
foreach my $cpu (@cpu_array) {
    $cpu_rsrc{$cpu} = 0;
}
if (-e "/tmp/vcore.log") {
    open FD, "< /tmp/vcore.log" or die "Cannot open /tmp/vcore.log for read!";
    while (<FD>) {
        my $line = $_;
        if ($line =~ /VCPU:([0-9]+)\s+/) {
            $cpu_rsrc{$1} = $cpu_rsrc{$1} + 1;
        }
    }
    close FD;
    my $min = 99;
    my $min_idx = -1;
    foreach my $cpu (keys %cpu_rsrc) {
        if ($cpu_rsrc{$cpu} < $min) {
            $min = $cpu_rsrc{$cpu};
            $min_idx = $cpu;
        }
    }
    print "$min_idx\n";
} else {
    print "0\n";
}
