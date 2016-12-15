#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV != 1) {
    print "Usage: ./query_last_yarn_app_id_in_some_state.pl <HADOOP_HOME> <CATEGORY>\n";
    exit 1;
}

my $hadoop_home = $ARGV[0];
my $category = $ARGV[1];

`$hadoop_home/bin/yarn application -appStates $category -list 2>&1 | tail -n 1 | grep Application-Id`;
if ($? == 0) {
    print "NONE";
    exit 0;
} else {
    my $info = `$hadoop_home/bin/yarn application -appStates $category -list 2>&1`;
    my @infos = split(/\n/, $info);
    my $line_interesting = "";
    my $get_this_line = 0;
    foreach my $line (@infos) {
        if ($get_this_line == 1) {
            $get_this_line = 0;
            $line_interesting = $line;
        }
        if ($line =~ /^\s+Application-Id/) {
            $get_this_line = 1;
        } elsif ($line =~ /\s+$category\s+/) {
            $get_this_line = 1;
        }
    }
    if ($line_interesting =~ /([\s+]?)([^\s]+)\s+/) {
        print $2;
    }
}
exit 0;

