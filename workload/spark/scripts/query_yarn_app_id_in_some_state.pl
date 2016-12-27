#!/usr/bin/perl
use strict;
use warnings;

if ($#ARGV < 1) {
    print "Usage: ./query_yarn_app_id_in_some_state.pl <HADOOP_HOME> <CATEGORY>\n";
    exit 1;
}

my $hadoop_home = $ARGV[0];
my $category = $ARGV[1];
my $debug_fn = "";
if ($#ARGV > 1) {
    $debug_fn = $ARGV[2];
}

`$hadoop_home/bin/yarn application -appStates $category -list 2>&1 | tail -n 1 | grep Application-Id`;
if ($? == 0) {
    print "NONE\n";
    exit 0;
} else {
    my $output = "";
    my $info = `$hadoop_home/bin/yarn application -appStates $category -list 2>&1`;
    my @infos = split(/\n/, $info);
    my $line_interesting = "";
    my $get_this_line = 0;
    foreach my $line (@infos) {
        if ($get_this_line == 1) {
            $get_this_line = 0;
            $line_interesting = $line;
            if ($line_interesting =~ /([\s+]?)([^\s]+)\s+/) {
                my $app_entry = $2;
                $output = $output.$app_entry."\n";
                if (($debug_fn ne "") and (not ($app_entry =~ /^app/))) {
                    `echo "query_yarn_app_id_in_some_state.pl $hadoop_home $category got suspect result:" >> $debug_fn`;
                    open DEBUG, ">> $debug_fn" or die "Cannot open file $debug_fn for append";
                    print DEBUG $app_entry."\n";
                    print DEBUG $info."\n\n";
                    close DEBUG;
                }
            }
        }
        if ($line =~ /^\s+Application-Id/) {
            $get_this_line = 1;
        } elsif ($line =~ /\s+$category\s+/) {
            $get_this_line = 1;
        }
    }
    print $output;
}
exit 0;

