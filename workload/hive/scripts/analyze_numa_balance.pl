#!/usr/bin/perl
use strict;
use warnings;

my $interval = 30;
my $count = 600;

if ($#ARGV == 1) {
    $interval = $ARGV[0];
    $count = $ARGV[1];
}

`echo "org.apache.hadoop.hdfs.server.datanode.DataNode" > task.log`;
`ps -ef | grep "org.apache.hadoop.hdfs.server.datanode.DataNode" | grep -v grep | awk '{print \$2}' | xargs -i taskset -p {} 2>&1 >> task.log`;
`echo "org.apache.hadoop.yarn.server.nodemanager.NodeManager" >> task.log`;
`ps -ef | grep "org.apache.hadoop.yarn.server.nodemanager.NodeManager" | grep -v grep | awk '{print \$2}' | xargs -i taskset -p {} 2>&1 >> task.log`;

my %mask_mapping = ();
my $node_idx = 0;
my $init_done = 0;
while ($count > 0) {
    `ps -ef | grep java | grep -v grep | grep -v "/bin/bash" | grep -v "org.apache.hadoop.hdfs.server.datanode.DataNode" | grep -v "org.apache.hadoop.yarn.server.nodemanager.NodeManager" > /tmp/analyze_numa_balance.status`;
    my $info = `cat /tmp/analyze_numa_balance.status | awk '{print \$2}' | xargs -i taskset -p {} 2>&1 | grep current`;
    my @lines = split(/\n/, $info);
    my %affinity_proc_cnt = ();
    my %affinity_vcore_cnt = ();
    my %pid_core = ();
    open FD, "< /tmp/analyze_numa_balance.status" or die "Cannot open /tmp/analyze_numa_balance.status for read!\n";
    while (<FD>) {
        my $line = $_;
        if ($line =~ /^[^\s]+\s+([0-9]+)/) {
            my $pid = $1;
            if ($line =~ /\s+--cores\s+([0-9]+)\s+/) {
                my $core = $1;
                $pid_core{$pid} = $core;
            }
        }
    }
    close FD;
    foreach my $line (@lines) {
        chomp($line);
        if ($line =~ /pid\s+([0-9]+)'s\s+current\s+affinity\s+mask:\s+([0-9a-f]+)$/) {
            my $pid = $1;
            my $affinity = $2;
            if (exists $affinity_proc_cnt{$affinity}) {
                $affinity_proc_cnt{$affinity} = $affinity_proc_cnt{$affinity} + 1;
                if (exists $pid_core{$pid}) {
                    $affinity_vcore_cnt{$affinity} = $affinity_vcore_cnt{$affinity} + $pid_core{$pid};
                } else {
                    $affinity_vcore_cnt{$affinity} = $affinity_vcore_cnt{$affinity} + 1;
                }
            } else {
                $affinity_proc_cnt{$affinity} = 1;
                if (exists $pid_core{$pid}) {
                    $affinity_vcore_cnt{$affinity} = $pid_core{$pid};
                } else {
                    $affinity_vcore_cnt{$affinity} = 1;
                }
                if ($init_done == 0) {
                    $mask_mapping{$affinity} = $node_idx;
                    $node_idx = $node_idx + 1;
                }
            }
        }
    }
    if ($init_done == 0) {
        foreach my $key (keys %affinity_proc_cnt) {
            #print "node".$mask_mapping{$key}." mask: ".$key."\n";
            open FD, ">> task.log" or die "Cannot open file task.log for append\n";
            print FD "node".$mask_mapping{$key}." mask: ".$key."\n";
            close FD;
        }
    }
    $init_done = 1;
    `date >> task.log`;
    foreach my $key (keys %affinity_proc_cnt) {
        open FD, ">> task.log" or die "Cannot open file task.log for append\n";
        print FD "node".$mask_mapping{$key}." ".$affinity_proc_cnt{$key}."/".$affinity_vcore_cnt{$key}."\t";
        close FD;
    }
    open FD, ">> task.log" or die "Cannot open file task.log for append\n";
    print FD "\n\n";
    close FD;
    `sleep $interval`;
    $count = $count - 1;
}
