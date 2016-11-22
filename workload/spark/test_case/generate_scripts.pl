#!/usr/bin/perl
use strict;
use warnings;
use lib qw(..);
use JSON qw( );
use File::Basename;

if ($#ARGV + 1 != 1) {
    die "Usage: ./generate_scripts.pl <TEST_CASE_TAG>";
}

my $case_tag = $ARGV[0];
if (not ((-e $case_tag."-spark-conf.json") and (-e $case_tag."-scenario.json"))) {
    die "Please define JSON files for $case_tag, take example*.json as reference!";
}
my $case_spark_conf_fn = $case_tag."-spark-conf.json";
my $case_scenario_fn = $case_tag."-scenario.json";

########## Load JSON definition from above two files ############
my $spark_conf_text = do {
    open(my $json_fh, "<:encoding(UTF-8)", $case_spark_conf_fn) or die "Cannot open $case_spark_conf_fn for read!";
    local $/;
    <$json_fh>
};
my $json = JSON->new;
my $spark_conf = $json->decode($spark_conf_text);
# Sanity check
if (not (exists $spark_conf->{"MASTER"} and exists $spark_conf->{"SPARK_HOME"} and exists $spark_conf->{"HADOOP_HOME"})) {
    die "Please define MASTER/SPARK_HOME/HADOOP_HOME in $case_spark_conf_fn";
}
if ($spark_conf->{"SCHEDULER"} ne "YARN") {
    die "Does not support ".$spark_conf->{"SCHEDULER"}." currently, only YARN mode supported";
}
# Add spark event configuration
my $spark_event_log_dir = "/tmp/sparkLogs";
if (exists $spark_conf->{"SPARK_DEFAULTS"}) {
    if (exists $spark_conf->{"SPARK_DEFAULTS"}->{"spark.eventLog.enabled"}) {
        $spark_conf->{"SPARK_DEFAULTS"}->{"spark.eventLog.enabled"} = "true";
    }
    if (exists $spark_conf->{"SPARK_DEFAULTS"}->{"spark.eventLog.dir"}) {
        $spark_event_log_dir = $spark_conf->{"SPARK_DEFAULTS"}->{"spark.eventLog.dir"};
    } else {
        $spark_conf->{"SPARK_DEFAULTS"}->{"spark.eventLog.dir"} = $spark_event_log_dir;
    }
} else {
    my %conf = ();
    $conf{"spark.eventLog.enabled"} = "true";
    $conf{"spark.eventLog.dir"} = $spark_event_log_dir;
    $spark_conf->{"SPARK_DEFAULTS"} = \%conf;
}

my $scenario_text = do {
    open(my $json_fh, "<:encoding(UTF-8)", $case_scenario_fn) or die "Cannot open $case_scenario_fn for read!";
    local $/;
    <$json_fh>
};
my $scenario = $json->decode($scenario_text);

########### Verify the environment as defined in the JSON files ############
# Check MASTE is current node
my $ping_result = `ping $spark_conf->{"MASTER"} -c 1`;
if ($? != 0) {
    die "Please make sure to run the script from ".$spark_conf->{"MASTER"};
}
`ping $spark_conf->{"MASTER"} -c 1 | head -n 1 | awk -F\\( '{print \$2}' | awk -F\\) '{print \$1}' | xargs -i sh -c "ifconfig | grep {}"`;
if ($? != 0) {
    die "Please make sure to run the script from ".$spark_conf->{"MASTER"};
}

# Get all nodes and check ssh/dstat etc
if (not (-e $spark_conf->{"HADOOP_HOME"}."/etc/hadoop/slaves")) {
    die "slaves file not found under ".$spark_conf->{"HADOOP_HOME"}."/etc/hadoop/ folder";
}
my $slaves_str = `grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves`;
my @nodes = split(/\n/, $slaves_str);
my $all_slaves = "";
my $first_slave = "";
my $slave_count = $#nodes + 1;
foreach my $node (@nodes) {
    chomp($node);
    if ($all_slaves eq "") {
        $all_slaves = $node;
    } else {
        $all_slaves = $all_slaves." ".$node;
    }
    if ($first_slave eq "") {
        $first_slave = $node;
    }
}
my $cores_online = `ssh $first_slave ppc64_cpu --cores-on`;
my $cores = 0;
if ($cores_online =~ /Number of cores online = ([0-9]+)$/) {
    $cores = $1;
}
my $total_cores_online = $cores * $slave_count;
if ($cores == 0) {
    die "Cannot get online cores info from \"ppc64_cpu --cores-on\"";
}
push (@nodes, $spark_conf->{"MASTER"});
if ($#nodes == 0) {
    die "Slave nodes not defined in ".$spark_conf->{"HADOOP_HOME"}."/etc/hadoop/slaves";
}
my $need_install_tools = 0;
my $ssh_problem = 0;
foreach my $node (@nodes) {
    `ssh $node date`;
    if ($? != 0) {
        $ssh_problem = 1;
        print "$node not reachable or passwordless not set\n";
    } else {
        `ssh $node mkdir $spark_event_log_dir > /dev/null 2>&1`;
        `ssh $node which dstat`;
        if ($? != 0) {
            $need_install_tools = 1;
            print "Please install dstat on $node\n";
        }
        `ssh $node which ppc64_cpu`;
        if ($? != 0) {
            $need_install_tools = 1;
            print "Please install ppc64_cpu on $node\n";
        }
    }
}
if ($ssh_problem != 0) {
    die "Please resolve ssh passwordless access first";
}
if ($need_install_tools != 0) {
    die "Install tools and try again";
}

# Upload lpcpu to the first slave node
if (not (-e "../../../lpcpu.tar.bz2")) {
    die "lpcpu.tar.bz2 not found in repository";
}
`scp ../../../lpcpu.tar.bz2 $nodes[0]:/root/`;
`ssh $nodes[0] "cd /root && tar xjf lpcpu.tar.bz2"`;

########### Generate the test scripts ############
# Header
my $date_str = `date +"%Y%m%d%H%M%S"`;
chomp($date_str);
my $script_dir = $case_tag."-".$date_str;
`mkdir $script_dir`;
my $current_dir = `pwd`;
chomp($current_dir);
my $script_dir_full = $current_dir."/".$script_dir;
open my $script_fh, "> $script_dir/run.sh" or die "Cannot open file ".$script_dir."/run.sh for write";

my $pmh = dirname(dirname(dirname(`pwd`)));
print $script_fh <<EOF;
#!/bin/bash
# This script is generated by generate_scripts.pl
export PMH=$pmh
export WORKLOAD_NAME=$script_dir
export DESCRIPTION="$script_dir"
export WORKLOAD_DIR="."      # The workload working directory
export MEAS_DELAY_SEC=1      # Delay between each measurement
export RUNDIR=\$(\${PMH}/setup-run.sh \$WORKLOAD_NAME)
mkdir \$RUNDIR/spark_events

# SLAVES config required by run-workload.sh
unset SLAVES
SLAVES="$all_slaves"
export SLAVES

cd \$PMH

EOF

# *-scenario.json steps
my $smt_reset = 0;
foreach my $step (@{$scenario}) {
    if (exists $step->{"ACTION"}) {
        if ($step->{"ACTION"} eq "CLEAR_SWAPPINESS") {
            print $script_fh <<EOF;
# ACTION $step->{"ACTION"}
echo 0 > /proc/sys/vm/swappiness
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "echo 0 > /proc/sys/vm/swappiness"

EOF
        } elsif (($step->{"ACTION"} eq "HDFS") or ($step->{"ACTION"} eq "YARN")) {
            my $script_name = "dfs";
            if ($step->{"ACTION"} eq "YARN") {
                $script_name = "yarn";
            }
            my $script_action = "";
            if (not (exists $step->{"PARAM"})) {
                close $script_fh;
                `rm -rf $script_dir_full`;
                die "ACTION:".$step->{"ACTION"}." require PARAM START or STOP";
            } elsif ($step->{"PARAM"} eq "START") {
                $script_action = "start";
            } elsif ($step->{"PARAM"} eq "STOP") {
                $script_action = "stop"; 
            } else {
                close $script_fh;
                `rm -rf $script_dir_full`;
                die "ACTION:".$step->{"ACTION"}." invalid PARAM ".$step->{"PARAM"}.", require PARAM START or STOP";
            }
            print $script_fh <<EOF;
# ACTION $step->{"ACTION"}:$step->{"PARAM"}
$spark_conf->{"HADOOP_HOME"}/sbin/$script_action-$script_name.sh

EOF
        } elsif ($step->{"ACTION"} eq "DROP_CACHE") {
            print $script_fh <<EOF;
# ACTION $step->{"ACTION"}
sync && echo 3 > /proc/sys/vm/drop_caches
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "sync && echo 3 > /proc/sys/vm/drop_caches"

EOF
        } elsif ($step->{"ACTION"} eq "WAIT") {
            my $sec = 5;
            if (exists $step->{"PARAM"}) {
                $sec = $step->{"PARAM"};
            }
            print $script_fh <<EOF;
# ACTION $step->{"ACTION"}:$sec
sleep $sec

EOF
        } else {
            close $script_fh;
            `rm -rf $script_dir_full`;
            die "ACTION:".$step->{"ACTION"}." is not supported!";
        }
    } elsif (exists $step->{"TAG"}) {
        my $smt_changed = 0;
        my $tag_idx = 0;
        if (not (exists $step->{"CMD"})) {
            close $script_fh;
            `rm -rf $script_dir_full`;
            die "Please define CMD section in TAG ".$step->{"TAG"};
        }
        my $repeat = 1;
        if (exists $step->{"REPEAT"}) {
            $repeat = $step->{"REPEAT"};
        }
        my $drop_cache_between_run = 0;
        if ((exists $step->{"DROP_CACHE_BETWEEN_REPEAT"}) and ($step->{"DROP_CACHE_BETWEEN_REPEAT"} eq "TRUE")) {
            $drop_cache_between_run = 1;
        }
        my $set_smt = 0;
        if (exists $step->{"SMT"}) {
            $smt_changed = 1;
            $smt_reset = 1;
            $set_smt = $step->{"SMT"};
        }
        # Calculate SMT setting if there is "--num-executors" and "--executor-cores" configured in CMD parameter
        if (exists $step->{"CMD"}->{"PARAM"}) {
            my $def_num_executors = 0;
            my $def_executor_cores = 0;
            foreach my $element (@{$step->{"CMD"}->{"PARAM"}}) {
                if ($element =~ /--num-executors\s+([0-9]+)/) {
                    $def_num_executors = $1;
                }
                if ($element =~ /--executor-cores\s+([0-9]+)/) {
                    $def_executor_cores = $1;
                }
            }
            if (($def_num_executors > 0) and ($def_executor_cores > 0)) {
                $smt_changed = 1;
                $smt_reset = 1;
                my $total_cores_required = $def_num_executors * $def_executor_cores;
                my $smt_ratio = ($total_cores_required * 1.0)/($total_cores_online * 1.0);
                if ($smt_ratio <= 1.0) {
                    $set_smt = 1;
                } elsif ($smt_ratio <= 2.0) {
                    $set_smt = 2;
                } elsif ($smt_ratio <= 4.0) {
                    $set_smt = 4;
                } elsif ($smt_ratio <= 8.0) {
                    $set_smt = 8;
                } else {
                    close $script_fh;
                    `rm -rf $script_dir_full`;
                    die "TAG ".$step->{"TAG"}." --num-executors X --executor-cores exceed available cores in all slaves";
                }
            }
        }
        print $script_fh <<EOF;
# TEST STEP:$step->{"TAG"}
EOF
        if ($smt_changed == 1) {
            print $script_fh <<EOF;
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ppc64_cpu --smt=$set_smt"
EOF
        }
        my $cmd = "";
        if ($step->{"CMD"}->{"COMMAND"} =~ /\<SPARK_HOME\>/) {
            $step->{"CMD"}->{"COMMAND"} =~ s/\<SPARK_HOME\>/$spark_conf->{"SPARK_HOME"}/;
        }
        $cmd = $cmd.$step->{"CMD"}->{"COMMAND"};
        my $def_conf = 0;
        if (exists $step->{"CMD"}->{"PARAM"}) {
            foreach my $element (@{$step->{"CMD"}->{"PARAM"}}) {
                if (ref($element) eq "HASH") {
                    if (exists $element->{"--conf"}) {
                        $def_conf = 1;
                        foreach my $conf (@{$element->{"--conf"}}) {
                            $cmd = $cmd." --conf ".$conf;
                        }
                        if (exists $spark_conf->{"SPARK_DEFAULTS"}) {
                            foreach my $key (keys %{$spark_conf->{"SPARK_DEFAULTS"}}) {
                                $cmd = $cmd." --conf ".$key."=".$spark_conf->{"SPARK_DEFAULTS"}->{$key};
                            }
                        }
                    }
                } else {
                    if ($element =~ /\<SPARK_HOME\>/) {
                        $element =~ s/\<SPARK_HOME\>/$spark_conf->{"SPARK_HOME"}/;
                    }
                    $cmd = $cmd." ".$element;
                }
            }
        }
        if ($def_conf == 0) {
            print "Spark by default disable event log, however we need that enabled to analyze the result. Did not find \"--conf\" section to add spark event log configuration, are you sure enabled that in spark-defaults.conf ? [Y/N]\n";
            my $input = <STDIN>;
            if (($input =~ /N/) or ($input =~ /n/)) {
                print "Not confirmed, please configure that and run the script again.\n";
                close $script_fh;
                `rm -rf $script_dir_full`;
                exit 1;
            }
        }
        if ($repeat == 1) {
            print $script_fh <<EOF;
export RUN_ID=\"$step->{"TAG"}-0\"
CMD=\"${cmd}\"
CMD=\"\${CMD} > \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER0.log 2>&1\"
export WORKLOAD_CMD=\${CMD}
\${PMH}/run-workload.sh
grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER0.log > /dev/null 2>&1
if [ $? -eq 0 ]
then
    TGT_EVENT_LOG_FN=`grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER0.log | awk -F"file:" '{print \$2}'`;
    DST_EVENT_LOG_FN=`grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER0.log | awk -F"file:" '{print \$2}' | awk -F/ '{print \$NF}'`;
    for SLAVE in \$SLAVES
    do
        scp \$SLAVE:\$TGT_EVENT_LOG_FN \$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER0 > /dev/null 2>&1
    done
else
    echo "################ Error, did not find event log info ##################"
fi

EOF
        } else {
            print $script_fh <<EOF;
for ITER in \$(seq $repeat)
do
EOF
            if ($drop_cache_between_run == 1) {
                print $script_fh <<EOF;
    if [ \$ITER -ne 1 ] 
    then
        sync && echo 3 > /proc/sys/vm/drop_caches
        grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "sync && echo 3 > /proc/sys/vm/drop_caches"
    fi
EOF
            }
            print $script_fh <<EOF;
    export RUN_ID=\"$step->{"TAG"}-\$ITER\"
    CMD=\"${cmd}\"
    CMD=\"\${CMD} > \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log 2>&1\"
    export WORKLOAD_CMD=\${CMD}
    \${PMH}/run-workload.sh
    grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log > /dev/null 2>&1
    if [ $? -eq 0 ]
    then
        TGT_EVENT_LOG_FN=`grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log | awk -F"file:" '{print \$2}'`;
        DST_EVENT_LOG_FN=`grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log | awk -F"file:" '{print \$2}' | awk -F/ '{print \$NF}'`;
        for SLAVE in \$SLAVES
        do
            scp \$SLAVE:\$TGT_EVENT_LOG_FN \$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER\$ITER > /dev/null 2>&1
        done
    else
        echo "################ Error, did not find event log info ##################"
    fi
done

EOF
        }
    }
}

# Restore SMT4 if SMT setting has been changed
if ($smt_reset == 1) {
    print $script_fh <<EOF;
# ACTION reset to SMT4 on all slave nodes
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ppc64_cpu --smt=4"

EOF
}

close $script_fh;
`chmod +x $script_dir/run.sh`;
