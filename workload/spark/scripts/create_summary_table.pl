#!/usr/bin/perl
use strict;
use warnings;
use lib qw(..);
use JSON qw( );
use File::Basename;

if ($#ARGV != 1) {
    print "Usage: ./create_summary_table.pl <JSON file> <RUNDIR>\n";
    exit 1;
}

my $test_plan_fn = $ARGV[0];
my $rundir = $ARGV[1];
my $info = $ARGV[1]."/../../../info";
my $scenario_text = do {
    open(my $json_fh, "<:encoding(UTF-8)", $test_plan_fn) or die "Cannot open $test_plan_fn for read!";
    local $/;
    <$json_fh>
};

# The default fields: run_id and count
my %actions = ();
my @run_id_array;
push @run_id_array, "<PMH>/workload/spark/scripts/summary_run_id.pl";
push @run_id_array, "<TAG>";
$actions{"run_id"} = \@run_id_array;
my @count_array;
push @count_array, "<PMH>/workload/spark/scripts/summary_count.pl";
push @count_array, "<TAG>";
push @count_array, "<INFO>";
$actions{"count"} = \@count_array;

my @fields = qw( run_id count );
my $json = JSON->new;
my $scenario = $json->decode($scenario_text);
foreach my $step (@{$scenario}) {
    if (exists $step->{"SUMMARY"}) {
        foreach my $key (keys %{$step->{"SUMMARY"}}) {
            push @fields, $key;
            $actions{$key} = $step->{"SUMMARY"}->{$key};
        }
        open my $summary_fh, "> $rundir/html/summary.html" or die "Cannot open file ".$rundir."/html/summary.html for write";
        print $summary_fh <<EOF;
<table>
<tr>
EOF
        foreach my $field (@fields) {
        print $summary_fh <<EOF;
<th>$field</th>
EOF
        }
        print $summary_fh <<EOF;
</tr>
EOF
        my $tag_str = `cat $info | grep COUNT | awk '{print \$1}' | awk -F: '{print \$2}'`;
        my @tags = split(/\n/, $tag_str);
        foreach my $tag (@tags) {
            print $summary_fh <<EOF;
<tr>
EOF
            foreach my $field (@fields) {
                my $cmd = "";
                foreach my $cmd_field (@{$actions{$field}}) {
                    $cmd = $cmd.$cmd_field." ";
                }
                # Substitute fields
                if ($cmd =~ /\<PMH\>/) {
                    $cmd =~ s/\<PMH\>/$ENV{'PMH'}/;
                }
                if ($cmd =~ /\<INFO\>/) {
                    $cmd =~ s/\<INFO\>/$info/;
                }
                if ($cmd =~ /\<TAG\>/) {
                    $cmd =~ s/\<TAG\>/$tag/;
                }
                my $cmd_output = `$cmd`;
                chomp($cmd_output);
                print $summary_fh <<EOF;
<td>$cmd_output</td>
EOF
            }
            print $summary_fh <<EOF;
</tr>
EOF
        }
        print $summary_fh <<EOF;
</table>
EOF
        close $summary_fh;
        exit 0;
    }
}
