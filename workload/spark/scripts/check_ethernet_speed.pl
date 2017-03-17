#!/usr/bin/perl
use warnings;
use strict;

if ($#ARGV != 3) {
    print "Usage: ./check_ethernet_speed.pl <hostname> <intf_name> <tgt_speed> <reboot_wait>\n";
    exit 1;
}
my $hostname = $ARGV[0];
my $intf = $ARGV[1];
my $tgt = $ARGV[2];
my $wait = $ARGV[3];

my $ret = `ssh -l root $hostname ethtool $intf | grep Speed`;
if ($ret =~ /Speed:\s+([0-9]+)/) {
    my $link_speed = $1;
    if ($link_speed == $tgt) {
        exit 0;
    } else {
        my $reboot_cnt = 5;
        while ($reboot_cnt > 0) {
            `ssh -l root $hostname reboot`;
            `sleep $wait`;
            $ret = `ssh -l root $hostname ethtool $intf | grep Speed`;
            if ($ret =~ /Speed:\s+([0-9]+)/) {
                $link_speed = $1;
                if ($link_speed == $tgt) {
                    exit 0;
                }
            } else {
                exit 0;
            }
            $reboot_cnt = $reboot_cnt - 1;
        }
    }
}
