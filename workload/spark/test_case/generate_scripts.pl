#!/usr/bin/perl
use strict;
use warnings;
use lib qw(..);
use JSON qw( );
use File::Basename;

if ($#ARGV + 1 != 1) {
    die "Usage: ./generate_scripts.pl <TEST_PLAN_JSON>";
}

my $is_power = 0;
`uname -a | grep ppc64le`;
if ($? == 0) {
    $is_power = 1;
}

my $test_plan_fn = $ARGV[0];
my %tags = ();
########## Load JSON definition from above two files ############
my $scenario_text = do {
    open(my $json_fh, "<:encoding(UTF-8)", $test_plan_fn) or die "Cannot open $test_plan_fn for read!";
    local $/;
    <$json_fh>
};
my $json = JSON->new;
my $scenario = $json->decode($scenario_text);
my $spark_conf_fn = "";
foreach my $step (@{$scenario}) {
    if (exists $step->{"USE"}) {
        $spark_conf_fn = $step->{"USE"};
    }
}
if ($spark_conf_fn eq "") {
    die "Please reference spark-conf.json using \"USE\" in your $test_plan_fn"
}
my $spark_conf_text = do {
    open(my $json_fh, "<:encoding(UTF-8)", $spark_conf_fn) or die "Cannot open $spark_conf_fn for read!";
    local $/;
    <$json_fh>
};
my $spark_conf = $json->decode($spark_conf_text);
# Sanity check
if (not (exists $spark_conf->{"MASTER"} and exists $spark_conf->{"SPARK_HOME"} and exists $spark_conf->{"HADOOP_HOME"})) {
    die "Please define MASTER/SPARK_HOME/HADOOP_HOME in $spark_conf_fn";
}
if (($spark_conf->{"SCHEDULER"} ne "YARN") and ($spark_conf->{"SCHEDULER"} ne "STANDALONE")) {
    die "Does not support ".$spark_conf->{"SCHEDULER"}.", only YARN/STANDALONE supported";
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

# Get the default spark event log dir from spark-defaults.conf if that is defined
my $default_spark_event_dir = "";
if (-e $spark_conf->{"SPARK_HOME"}."/conf/spark-defaults.conf") {
    `grep spark.eventLog.dir $spark_conf->{"SPARK_HOME"}/conf/spark-defaults.conf`;
    if ($? == 0) {
        $default_spark_event_dir = `grep spark.eventLog.dir $spark_conf->{"SPARK_HOME"}/conf/spark-defaults.conf | awk '{print \$2}'`;
        chomp($default_spark_event_dir);
    }
}

########### Verify the environment as defined in the JSON files ############
# Check MASTE is current node
my $ping_result = `ping $spark_conf->{"MASTER"} -c 1`;
if ($? != 0) {
    die "Please make sure to run the script from ".$spark_conf->{"MASTER"};
}
`ping $spark_conf->{"MASTER"} -c 1 | head -n 1 | awk -F\\( '{print \$2}' | awk -F\\) '{print \$1}' | xargs -i sh -c "ifconfig | grep {}"`;
my $master_ip = `ping $spark_conf->{"MASTER"} -c 1 | head -n 1 | awk -F\\( '{print \$2}' | awk -F\\) '{print \$1}'`;
chomp($master_ip);
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

my $cores_online = "";
my $cores = 0;
my $total_cores_online = 0;
if ($is_power == 1) {
    $cores_online = `ssh $first_slave ppc64_cpu --cores-on`;
    if ($cores_online =~ /Number of cores online = ([0-9]+)$/) {
        $cores = $1;
    }
    $total_cores_online = $cores * $slave_count;
    if ($cores == 0) {
        die "Cannot get online cores info from \"ppc64_cpu --cores-on\"";
    }
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
        if ($is_power == 1) {
            `ssh $node which ppc64_cpu`;
            if ($? != 0) {
                $need_install_tools = 1;
                print "Please install ppc64_cpu on $node\n";
            }
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
my $case_tag = $test_plan_fn;
if ($case_tag =~ /(.*).json/) {
    $case_tag = $1;
}
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
ctrl_c_exit() {
    echo "Cleanup environment before exit now"
    if [ \$CMD_TO_KILL != "" ]
    then
        echo "Got command to kill \$CMD_TO_KILL"
        `ps -ef | grep "\$CMD_TO_KILL" | grep -v grep | awk '{print \$2}' | xargs -i kill -9 {}`
        `ps -ef | grep java | grep SparkSubmit | awk '{print \$2}' | xargs -i kill -9 {}`
EOF

# Restore SMT4 if SMT setting has been changed
if ($is_power == 1) {
    print $script_fh <<EOF;
        if [ \$SMT_NEED_RESET -eq 1 ]
        then
            # ACTION reset to SMT4 on all slave nodes
            grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ppc64_cpu --smt=4"
        fi
EOF
}

print $script_fh <<EOF;
        \$PMH/workload/spark/scripts/create_summary_table.pl \$PMH/workload/spark/test_case/$test_plan_fn \$RUNDIR

EOF

# Restore spark-env.sh if we are running in STANDALONE mode
if ($spark_conf->{"SCHEDULER"} eq "STANDALONE") {
    print $script_fh <<EOF;
        $spark_conf->{"SPARK_HOME"}/sbin/stop-all.sh
        \\cp \$RUNDIR/.spark-env.sh.backup.master $spark_conf->{"SPARK_HOME"}/conf/spark-env.sh
        for SLAVE in \$SLAVES
        do
            scp \$RUNDIR/.spark-env.sh.backup.\$SLAVE \$SLAVE:$spark_conf->{"SPARK_HOME"}/conf/spark-env.sh
        done
EOF
}

print $script_fh <<EOF;
    fi
    exit 1
}

EOF

print $script_fh <<EOF;
CMD_TO_KILL=""
SMT_NEED_RESET=0
trap ctrl_c_exit INT

export PMH=$pmh
export WORKLOAD_NAME=$script_dir
export DESCRIPTION="$script_dir"
export WORKLOAD_DIR="."      # The workload working directory
export MEAS_DELAY_SEC=1      # Delay between each measurement
export RUNDIR=\$(\${PMH}/setup-run.sh \$WORKLOAD_NAME)
mkdir \$RUNDIR/spark_events
INFO=\$PMH/workload/spark/test_case/$script_dir/info
APPID=\$PMH/workload/spark/test_case/$script_dir/appid
DEBUG=\$PMH/workload/spark/test_case/$script_dir/debug.log
rm -f \$INFO

# SLAVES config required by run-workload.sh
unset SLAVES
SLAVES="$all_slaves"
export SLAVES

cd \$PMH
cp -R html \$RUNDIR/html

EOF

# Backup spark-env.sh if we are running in STANDALONE mode
if ($spark_conf->{"SCHEDULER"} eq "STANDALONE") {
    print $script_fh <<EOF;
cp $spark_conf->{"SPARK_HOME"}/conf/spark-env.sh \$RUNDIR/.spark-env.sh.backup.master
for SLAVE in \$SLAVES
do
    scp \$SLAVE:$spark_conf->{"SPARK_HOME"}/conf/spark-env.sh \$RUNDIR/.spark-env.sh.backup.\$SLAVE
done
if [ ! -e $spark_conf->{"SPARK_HOME"}/conf/slaves ]
then
    cp $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves $spark_conf->{"SPARK_HOME"}/conf/slaves
fi

EOF
}

# *-scenario.json steps
my $smt_reset = 0;
my $last_worker_instances = 0;
my $last_worker_cores = 0;
my $def_worker_instances = 0;
my $def_worker_cores = 0;
my $change_smt = 1;
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
        my $current_spark_event_dir = "";
        my $smt_changed = 0;
        my $tag_idx = 0;
        if (exists $tags{$step->{"TAG"}}) {
            $tags{$step->{"TAG"}} = $tags{$step->{"TAG"}} + 1;
            $step->{"TAG"} = $step->{"TAG"}."_".$tags{$step->{"TAG"}};
        } else {
            $tags{$step->{"TAG"}} = 1;
        }
        if (not (exists $step->{"CMD"})) {
            close $script_fh;
            `rm -rf $script_dir_full`;
            die "Please define CMD section in TAG ".$step->{"TAG"};
        }
        my $repeat = 1;
        if (exists $step->{"REPEAT"}) {
            $repeat = $step->{"REPEAT"};
        }
        # Default do drop cache between runs
        my $drop_cache_between_run = 1;
        if ((exists $step->{"DROP_CACHE_BETWEEN_REPEAT"}) and ($step->{"DROP_CACHE_BETWEEN_REPEAT"} eq "FALSE")) {
            $drop_cache_between_run = 0;
        }
        my $skip_standalone_worker_restart = 0;
        if (not ((exists $step->{"CMD"}->{"EXECUTOR_PER_DN"}) and (exists $step->{"CMD"}->{"EXECUTOR_VCORES"}))) {
            $skip_standalone_worker_restart = 1;
        } else {
            $def_worker_instances = $step->{"CMD"}->{"EXECUTOR_PER_DN"};
            $def_worker_cores = $step->{"CMD"}->{"EXECUTOR_VCORES"};
            if (($is_power == 1) and ($change_smt == 1)) {
                my $set_smt = 0;
                if (exists $step->{"SMT"}) {
                    $smt_changed = 1;
                    $smt_reset = 1;
                    $set_smt = $step->{"SMT"};
                } else {
                    $smt_changed = 1;
                    $smt_reset = 1;

                    # For YARN, we need to add additional core for Application Master
                    if ($spark_conf->{"SCHEDULER"} eq "YARN") {
                        $def_worker_cores = $def_worker_cores + 1;
                    }
                    my $node_cores_required = $def_worker_instances * $def_worker_cores;
                    my $smt_ratio = ($node_cores_required * 1.0)/($cores * 1.0);
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
                        die "TAG ".$step->{"TAG"}." EXECUTOR_PER_DN(".$def_worker_instances.") X EXECUTOR_VCORES(".$def_worker_cores.") exceed available cores in all slaves";
                    }

                    # For YARN, we need to restore the original core config
                    if ($spark_conf->{"SCHEDULER"} eq "YARN") {
                        $def_worker_cores = $def_worker_cores - 1;
                    }
                }
                print $script_fh <<EOF;
# TEST STEP:$step->{"TAG"}
EOF
                if ($smt_changed == 1) {
                    print $script_fh <<EOF;
echo "SET SMT to $set_smt on all slaves"
SMT_NEED_RESET=1
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ppc64_cpu --smt=$set_smt"
EOF
                }
            }
        }

        # For standalone mode, need to update spark-env.sh with ENV, then restart master/slaves
        if (($spark_conf->{"SCHEDULER"} eq "STANDALONE") and (($last_worker_instances != $def_worker_instances) or ($last_worker_cores != $def_worker_cores)) and ($skip_standalone_worker_restart == 0)) {
            print $script_fh <<EOF;
$spark_conf->{"SPARK_HOME"}/sbin/stop-all.sh
\\cp \$RUNDIR/.spark-env.sh.backup.master $spark_conf->{"SPARK_HOME"}/conf/spark-env.sh
echo "export SPARK_WORKER_INSTANCES=$def_worker_instances" >> $spark_conf->{"SPARK_HOME"}/conf/spark-env.sh
echo "export SPARK_WORKER_CORES=$def_worker_cores" >> $spark_conf->{"SPARK_HOME"}/conf/spark-env.sh
EOF
            if (exists $step->{"CMD"}->{"EXECUTOR_MEM"}) {
                print $script_fh <<EOF;
echo "export SPARK_WORKER_MEMORY=$step->{"CMD"}->{"EXECUTOR_MEM"}" >> $spark_conf->{"SPARK_HOME"}/conf/spark-env.sh
EOF
            }
            print $script_fh <<EOF;
echo "export SPARK_MASTER_IP=$master_ip" >> $spark_conf->{"SPARK_HOME"}/conf/spark-env.sh
for SLAVE in \$SLAVES
do
    \\cp \$RUNDIR/.spark-env.sh.backup.\$SLAVE /tmp/spark-env.sh.backup.\$SLAVE
    echo "export SPARK_WORKER_INSTANCES=$def_worker_instances" >> /tmp/spark-env.sh.backup.\$SLAVE
    echo "export SPARK_WORKER_CORES=$def_worker_cores" >> /tmp/spark-env.sh.backup.\$SLAVE
EOF
            if (exists $step->{"CMD"}->{"EXECUTOR_MEM"}) {
                print $script_fh <<EOF;
    echo "export SPARK_WORKER_MEMORY=$step->{"CMD"}->{"EXECUTOR_MEM"}" >> /tmp/spark-env.sh.backup.\$SLAVE
EOF
            }
            print $script_fh <<EOF;
    echo "export SPARK_MASTER_IP=$master_ip" >> /tmp/spark-env.sh.backup.\$SLAVE
    scp /tmp/spark-env.sh.backup.\$SLAVE \$SLAVE:$spark_conf->{"SPARK_HOME"}/conf/spark-env.sh
done
$spark_conf->{"SPARK_HOME"}/sbin/start-all.sh
EOF
        }

        my $cmd = "";
        if ($step->{"CMD"}->{"COMMAND"} =~ /\<SPARK_HOME\>/) {
            $step->{"CMD"}->{"COMMAND"} =~ s/\<SPARK_HOME\>/$spark_conf->{"SPARK_HOME"}/;
        }
        print $script_fh <<EOF;
CMD_TO_KILL="$step->{"CMD"}->{"COMMAND"}"
EOF
        $cmd = $cmd.$step->{"CMD"}->{"COMMAND"};
        my $def_conf = 0;
        if ($spark_conf->{"SCHEDULER"} eq "STANDALONE") {
            $cmd = $cmd." --master spark://$master_ip:7077";
            if (exists $step->{"CMD"}->{"EXECUTOR_MEM"}) {
                $cmd = $cmd." --conf spark.executor.memory=".$step->{"CMD"}->{"EXECUTOR_MEM"};
            }
            if (exists $step->{"CMD"}->{"DRIVER_MEM"}) {
                $cmd = $cmd." --conf spark.driver.memory=".$step->{"CMD"}->{"DRIVER_MEM"};
            }
        } else {
            $cmd = $cmd." --master yarn";
            if (exists $step->{"CMD"}->{"EXECUTOR_PER_DN"}) {
                my $total_executors = $slave_count * $def_worker_instances;
                $cmd = $cmd." --num-executors $total_executors";
            }
            $cmd = $cmd." --executor-cores ".$step->{"CMD"}->{"EXECUTOR_VCORES"};
            if (exists $step->{"CMD"}->{"EXECUTOR_MEM"}) {
                $cmd = $cmd." --executor-memory ".$step->{"CMD"}->{"EXECUTOR_MEM"};
            }
            if (exists $step->{"CMD"}->{"DRIVER_MEM"}) {
                $cmd = $cmd." --driver-memory ".$step->{"CMD"}->{"DRIVER_MEM"};
            }
        }
        if (exists $step->{"CMD"}->{"PARAM"}) {
            foreach my $element (@{$step->{"CMD"}->{"PARAM"}}) {
                if (ref($element) eq "HASH") {
                    if (exists $element->{"--conf"}) {
                        $def_conf = 1;
                        $current_spark_event_dir = $spark_event_log_dir;
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
                    if ($element =~ /\<SPARK_MASTER_IP\>/) {
                        $element =~ s/\<SPARK_MASTER_IP\>/$master_ip/;
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
            if ($default_spark_event_dir eq "") {
                $current_spark_event_dir = "/tmp/spark-events";
            } else {
                $current_spark_event_dir = $default_spark_event_dir;
            }
        }

        print $script_fh <<EOF;
echo \"TAG:$step->{"TAG"} COUNT:$repeat\" >> \$INFO
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
    export RUN_ID=\"$step->{"TAG"}-ITER\$ITER\"
    CMD=\"${cmd}\"
    CMD=\"\${CMD} > \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log 2>&1\"
    export WORKLOAD_CMD=\${CMD}
EOF
        # For YARN scheduler, get the latest FINISHED/FAILED/KILLED application-id
        if ($spark_conf->{"SCHEDULER"} eq "YARN") {
            print $script_fh <<EOF;
    # Get existing application-id infos
    echo "FINISHED" > \$APPID
    `\$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl $spark_conf->{"HADOOP_HOME"} FINISHED \$DEBUG >> \$APPID`;
    echo "FAILED" >> \$APPID
    `\$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl $spark_conf->{"HADOOP_HOME"} FAILED \$DEBUG >> \$APPID`;
    echo "KILLED" >> \$APPID
    `\$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl $spark_conf->{"HADOOP_HOME"} KILLED \$DEBUG >> \$APPID`;
    echo "RUNNING" >> \$APPID
    `\$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl $spark_conf->{"HADOOP_HOME"} RUNNING \$DEBUG >> \$APPID`;
    \$PMH/workload/spark/scripts/query_yarn_app_id.pl \$APPID \$INFO $step->{"TAG"} \$ITER $spark_conf->{"HADOOP_HOME"} \$PMH/workload/spark/scripts \$DEBUG &
EOF
        }
        print $script_fh <<EOF;
    \${PMH}/run-workload.sh
    DURATION=`grep "Elapsed (wall clock) time" \$RUNDIR/data/raw/$step->{"TAG"}-ITER\${ITER}_time_stdout.txt | awk -F"m:ss): " '{print \$2}' | awk -F: 'END { if (NF == 2) {sum=\$1*60+\$2} else {sum=\$1*3600+\$2*60+\$3} print sum}'`
    echo \"TAG:$step->{"TAG"} ITER:\$ITER DURATION:\$DURATION\" >> \$INFO
EOF
        if ($spark_conf->{"SCHEDULER"} eq "STANDALONE") {
            print $script_fh <<EOF;
    grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log > /dev/null 2>&1
    if [ \$? -eq 0 ]
    then
        TGT_EVENT_LOG_FN=`grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log | awk -F"file:" '{print \$2}'`;
        DST_EVENT_LOG_FN=`grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log | awk -F"file:" '{print \$2}' | awk -F/ '{print \$NF}'`;
        echo \"TAG:$step->{"TAG"} ITER:\$ITER APPID:\$DST_EVENT_LOG_FN\" >> \$INFO
        echo \"TAG:$step->{"TAG"} ITER:\$ITER EVENTLOG:\$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER\$ITER\" >> \$INFO
        for SLAVE in \$SLAVES
        do
            scp \$SLAVE:\$TGT_EVENT_LOG_FN \$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER\$ITER > /dev/null 2>&1
        done
        scp $spark_conf->{"MASTER"}:\$TGT_EVENT_LOG_FN \$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER\$ITER > /dev/null 2>&1
    else
        grep "Submitted application" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log > /dev/null 2>&1
        if [ \$? -eq 0 ]
        then
            DST_EVENT_LOG_FN=`grep "Submitted application" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log | awk '{print \$NF}'`;
            for SLAVE in \$SLAVES
            do
                scp \$SLAVE:$current_spark_event_dir/\$DST_EVENT_LOG_FN \$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER\$ITER > /dev/null 2>&1
            done
            scp $spark_conf->{"MASTER"}:$current_spark_event_dir/\$DST_EVENT_LOG_FN \$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER\$ITER > /dev/null 2>&1
            echo \"TAG:$step->{"TAG"} ITER:\$ITER APPID:\$DST_EVENT_LOG_FN\" >> \$INFO
            echo \"TAG:$step->{"TAG"} ITER:\$ITER EVENTLOG:\$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER\$ITER\" >> \$INFO
        else
            echo "Cannot find app-ID, please enable console INFO log level when using STANDALONE scheduler!"
        fi
    fi
EOF
        } else {
            print $script_fh <<EOF;
    APP_ID_FROM_LOG=""
    grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log > /dev/null 2>&1
    if [ \$? -eq 0 ]
    then
        APP_ID_FROM_LOG=`grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log | awk -F"file:" '{print \$2}' | awk -F/ '{print \$NF}'`;
    else
        grep "Submitted application" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log > /dev/null 2>&1
        if [ \$? -eq 0 ]
        then
            APP_ID_FROM_LOG=`grep "Submitted application" \$PMH/workload/spark/test_case/$script_dir/$step->{"TAG"}-ITER\$ITER.log | awk '{print \$NF}'`;
        fi
    fi

    if [ \$APP_ID_FROM_LOG != "" ]
    then
        grep "TAG:$step->{"TAG"} ITER:\$ITER APPID:\$APP_ID_FROM_LOG" \$INFO > /dev/null 2>&1
        if [ \$? -ne 0 ]
        then
            sed -i "s/TAG:$step->{"TAG"} ITER:\$ITER APPID:.*\\\$/TAG:$step->{"TAG"} ITER:\$ITER APPID:\$APP_ID_FROM_LOG/g" \$INFO
        fi
    fi

    grep "TAG:$step->{"TAG"} ITER:\$ITER APPID:" \$INFO > /dev/null 2>&1
    while [ \$? -ne 0 ]
    do
        sleep 1
        grep "TAG:$step->{"TAG"} ITER:\$ITER APPID:" \$INFO > /dev/null 2>&1
    done
    grep "TAG:$step->{"TAG"} ITER:\$ITER APPID:TIMEOUT" \$INFO > /dev/null 2>&1
    if [ \$? -ne 0 ]
    then
        DST_EVENT_LOG_FN=`grep "TAG:$step->{"TAG"} ITER:\$ITER APPID:" \$INFO | awk -F\"APPID:\" '{print \$2}'`;
        for SLAVE in \$SLAVES
        do
            scp \$SLAVE:$current_spark_event_dir/\$DST_EVENT_LOG_FN \$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER\$ITER > /dev/null 2>&1
        done
        scp $spark_conf->{"MASTER"}:$current_spark_event_dir/\$DST_EVENT_LOG_FN \$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER\$ITER > /dev/null 2>&1
        echo \"TAG:$step->{"TAG"} ITER:\$ITER EVENTLOG:\$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$step->{"TAG"}-ITER\$ITER\" >> \$INFO
        $spark_conf->{"HADOOP_HOME"}/bin/yarn application -appStates FINISHED -list 2>&1 | grep \$DST_EVENT_LOG_FN > /dev/null 2>&1
        if [ \$? -eq 0 ]
        then
            echo \"TAG:$step->{"TAG"} ITER:\$ITER STATUS:0\" >> \$INFO
        else
            echo \"TAG:$step->{"TAG"} ITER:\$ITER STATUS:1\" >> \$INFO
        fi
        # FIXME: put time result into INFO
    else
        echo "Application ID not found for TAG:$step->{"TAG"} ITER:\$ITER"
    fi
EOF
        }
        if (exists $step->{"AFTER"}) {
            if ($step->{"AFTER"} =~ /\<HADOOP_HOME\>/) {
                $step->{"AFTER"} =~ s/\<HADOOP_HOME\>/$spark_conf->{"HADOOP_HOME"}/;
            }
            if ($step->{"AFTER"} =~ /\<SPARK_HOME\>/) {
                $step->{"AFTER"} =~ s/\<SPARK_HOME\>/$spark_conf->{"SPARK_HOME"}/;
            }
            print $script_fh <<EOF;
    # AFTER command
    $step->{"AFTER"}
done

EOF
        } else {
            print $script_fh <<EOF;
done

EOF
        }
        $last_worker_instances = $def_worker_instances;
        $last_worker_cores = $def_worker_cores;
    } elsif (exists $step->{"SHELL"}) {
        if ($step->{"SHELL"} =~ /\<HADOOP_HOME\>/) {
            $step->{"SHELL"} =~ s/\<HADOOP_HOME\>/$spark_conf->{"HADOOP_HOME"}/;
        }
        if ($step->{"SHELL"} =~ /\<SPARK_HOME\>/) {
            $step->{"SHELL"} =~ s/\<SPARK_HOME\>/$spark_conf->{"SPARK_HOME"}/;
        }
        print $script_fh <<EOF;
# SHELL command
$step->{"SHELL"}

EOF
    } elsif (exists $step->{"CHANGE_SMT"}) {
        if ($step->{"CHANGE_SMT"} eq "YES") {
            $change_smt = 1;
        } elsif ($step->{"CHANGE_SMT"} eq "NO") {
            $change_smt = 0;
        }
    }
}

# Restore SMT4 if SMT setting has been changed
if (($is_power == 1) and ($smt_reset == 1)) {
    print $script_fh <<EOF;
# ACTION reset to SMT4 on all slave nodes
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ppc64_cpu --smt=4"
EOF
}

print $script_fh <<EOF;
\$PMH/workload/spark/scripts/create_summary_table.pl \$PMH/workload/spark/test_case/$test_plan_fn \$RUNDIR

EOF

# Restore spark-env.sh if we are running in STANDALONE mode
if ($spark_conf->{"SCHEDULER"} eq "STANDALONE") {
    print $script_fh <<EOF;
$spark_conf->{"SPARK_HOME"}/sbin/stop-all.sh
\\cp \$RUNDIR/.spark-env.sh.backup.master $spark_conf->{"SPARK_HOME"}/conf/spark-env.sh
for SLAVE in \$SLAVES
do
    scp \$RUNDIR/.spark-env.sh.backup.\$SLAVE \$SLAVE:$spark_conf->{"SPARK_HOME"}/conf/spark-env.sh
done

EOF
}

close $script_fh;
`chmod +x $script_dir/run.sh`;
