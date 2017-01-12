#!/usr/bin/perl
use warnings;
use strict;
use POSIX qw(ceil);

if ($#ARGV != 1) {
    print "Usage: do_allocate_vcore.pl <CORE_COUNT> <CONTAINER_ID_STR>\n";
    exit 1;
}

my $alloc_req = $ARGV[0];
my $container_id_str = $ARGV[1];
my @node_id_array = ();
my @node_alloc_array = ();
my %node_seq_hash = ();
my %node_idx_hash = ();
my $cpus = `numactl -H | grep cpus | awk -F":" '{print \$2}' | tr ' ' '\\n'`;
my @list = split(/\n/, $cpus);
my @new_id_node = ();
my @new_alloc_node = ();
my $ref_id = \@new_id_node;
my $ref_alloc = \@new_alloc_node;
my $node_array_idx = -1;
my $node_idx = 0;
my $node_count = 0;
foreach my $element (@list) {
    if ($element =~ /^\s?$/) {
        if (@{$ref_id}) {
            push @node_id_array, $ref_id;
            push @node_alloc_array, $ref_alloc;
            $node_count = $node_count + 1;
        }
        my @new_id_node = ();
        my @new_alloc_node = ();
        $ref_id = \@new_id_node;
        $ref_alloc = \@new_alloc_node;
        $node_array_idx = $node_array_idx + 1;
        $node_idx = 0;
    } else {
        chomp($element);
        push @{$ref_id}, $element;
        push @{$ref_alloc}, 0;
        $node_seq_hash{$element} = $node_array_idx;
        $node_idx_hash{$element} = $node_idx;
        $node_idx = $node_idx + 1;
    }
}
if (@{$ref_id}) {
    push @node_id_array, $ref_id;
    push @node_alloc_array, $ref_alloc;
    $node_count = $node_count + 1;
}

my $result_node_str = "";
if (-e "/tmp/vcore.log") {
    my @node_usage = ();
    my $idx = 0;
    while ($idx < $node_count) {
        push @node_usage, 0;
        $idx = $idx + 1;
    }
    # Calculate existing usage
    open FD, "< /tmp/vcore.log" or die "Cannot open /tmp/vcore.log for read!";
    while (<FD>) {
        my $line = $_;
        if ($line =~ /VCPU:([0-9,]+)\s+COUNT:([0-9]+)\s+/) {
            my $str_pattern = $1;
            my $count = $2;
            my @cores = split(/,/, $str_pattern);
            if ($count != ($#cores + 1)) {
                `echo "Error cpu list count does not match" >> /tmp/error.log`;
            }
            my $initial_node_idx = -1;
            foreach my $core (@cores) {
                if ($initial_node_idx == -1) {
                    $initial_node_idx = $node_seq_hash{$core};
                } elsif ($node_seq_hash{$core} != $initial_node_idx) {
                    `echo "Error found cross node allocation!" >> /tmp/error.log`;
                }
                ${$node_alloc_array[$initial_node_idx]}[$node_idx_hash{$core}] = ${$node_alloc_array[$initial_node_idx]}[$node_idx_hash{$core}] + 1;
            }
            $node_usage[$initial_node_idx] = $node_usage[$initial_node_idx] + $count;
        }
    }
    close FD;
    # Get the min usage node
    my $min_usage = 1000;
    my $min_usage_idx = -1;
    $idx = 0;
    while ($idx < $node_count) {
        if ($node_usage[$idx] < $min_usage) {
            $min_usage = $node_usage[$idx];
            $min_usage_idx = $idx;
        }
        $idx = $idx + 1;
    }
    my $group_size = scalar(@{$node_id_array[$min_usage_idx]});
    my $group_count = ceil($group_size/$alloc_req);
    my @group_usage = ();
    $idx = 0;
    while ($idx < $group_count) {
        my $start_idx = $alloc_req * $idx;
        my $range_usage = 0;
        my $i = 0;
        while ($i < $alloc_req) {
            $range_usage = $range_usage + ${$node_alloc_array[$min_usage_idx]}[($start_idx + $i) % $group_size];
            $i = $i + 1;
        }
        push @group_usage, $range_usage;
        $idx = $idx + 1;
    }
    my $min_usage_node = $min_usage_idx;
    $min_usage = 1000;
    $min_usage_idx = -1;
    $idx = 0;
    while ($idx < $group_count) {
        if ($group_usage[$idx] < $min_usage) {
            $min_usage = $group_usage[$idx];
            $min_usage_idx = $idx;
        }
        $idx = $idx + 1;
    }
    $idx = 0;
    my $start_idx = $alloc_req * $min_usage_idx;
    while ($idx < $alloc_req) {
        $result_node_str = $result_node_str.${$node_id_array[$min_usage_node]}[($start_idx + $idx) % $group_size];
        if ($idx != ($alloc_req - 1)) {
            $result_node_str = $result_node_str.",";
        }
        $idx = $idx + 1;
    }
} else {
    my $idx = 0;
    my $group_size = scalar(@{$node_id_array[0]});
    my $start_idx = 0;
    while ($idx < $alloc_req) {
        $result_node_str = $result_node_str.${$node_id_array[0]}[($start_idx + $idx) % $group_size];
        if ($idx != ($alloc_req - 1)) {
            $result_node_str = $result_node_str.",";
        }
        $idx = $idx + 1;
    }
}
`echo "VCPU:$result_node_str COUNT:$alloc_req CONTAINER:$container_id_str" >> /tmp/vcore.log`;
print "$result_node_str\n";

exit 0;
