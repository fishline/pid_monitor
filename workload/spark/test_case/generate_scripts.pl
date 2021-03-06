#!/usr/bin/perl
use strict;
use warnings;
use lib qw(..);
use JSON qw( );
use File::Basename;

if ($#ARGV + 1 != 1) {
    die "Usage: ./generate_scripts.pl <TEST_PLAN_JSON>";
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
if ($spark_conf->{"SCHEDULER"} ne "YARN") {
    die "Does not support ".$spark_conf->{"SCHEDULER"}.", only YARN supported";
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
#`scp ../../../lpcpu.tar.bz2 $nodes[0]:/root/`;
#`ssh $nodes[0] "cd /root && tar xjf lpcpu.tar.bz2"`;

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
`cp $test_plan_fn $script_dir/`;
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

print $script_fh <<EOF;
        \$PMH/workload/spark/scripts/create_summary_table.pl \$PMH/workload/spark/test_case/$test_plan_fn \$RUNDIR

EOF

print $script_fh <<EOF;
    fi
    exit 1
}

EOF

print $script_fh <<EOF;
CMD_TO_KILL=""
SMT_NEED_RESET=0
trap ctrl_c_exit INT

export TAG_TO_KILL_ME=1
export KILL_TGT=`whoami`
export SPARK_HOME=$spark_conf->{"SPARK_HOME"}
export HADOOP_HOME=$spark_conf->{"HADOOP_HOME"}
export HADOOP_CONF_DIR=$spark_conf->{"HADOOP_HOME"}/etc/hadoop
export PMH=$pmh
export WORKLOAD_NAME=$script_dir
export DESCRIPTION="$script_dir"
export WORKLOAD_DIR="."      # The workload working directory
export MEAS_DELAY_SEC=1      # Delay between each measurement
export RUNDIR=\$(\${PMH}/setup-run.sh \$WORKLOAD_NAME)
export MASTER=$spark_conf->{"MASTER"}
mkdir \$RUNDIR/spark_events
mkdir \$RUNDIR/nmon
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

# *-scenario.json steps
my $last_worker_instances = 0;
my $last_worker_cores = 0;
my $def_worker_instances = 0;
my $def_worker_cores = 0;
my $sweeping = 0;
my $sweeping_params = "";
my $tag = "";
foreach my $step (@{$scenario}) {
    print $script_fh <<EOF;
# Do cleanup before any step
PIDS=`ps -ef | grep -v "ps -ef" | grep -v "ResourceManager" | grep "^\$KILL_TGT\\\\s" | awk '{print \$2}' | xargs -i sh -c "echo {} && cat /proc/{}/environ 2>/dev/null | grep -s TAG_TO_KILL_ME 2>&1" | grep -B 1 "Binary" | grep -v Binary | grep -v "\\\\-" | tr '\\n' ','`
echo \$PIDS | tr ',' '\\n' | grep -v "^\\\$" | xargs -i sh -c "kill -9 {} 2>/dev/null"
sleep 2
PIDS=`ps -ef | grep -v "ps -ef" | grep -v "ResourceManager" | grep "^\$KILL_TGT\\\\s" | awk '{print \$2}' | xargs -i sh -c "echo {} && cat /proc/{}/environ 2>/dev/null | grep -s TAG_TO_KILL_ME 2>&1" | grep -B 1 "Binary" | grep -v Binary | grep -v "\\\\-" | tr '\\n' ','`
echo \$PIDS | tr ',' '\\n' | grep -v "^\\\$" | xargs -i sh -c "kill -9 {} 2>/dev/null"

EOF
    # For SWEEPING, create new run.sh for scheduler
    if (exists $step->{"SWEEPING"}) {
        if ((not exists $step->{"SWEEPING_PARAMS"}) or (not exists $step->{"SWEEPING_METRIC"})) {
            die "SWEEPING block must have SWEEPING_PARAMS and SWEEPING_METRIC defined!";
        }
        $sweeping = 1;
        print $script_fh <<EOF;
# SWEEPING configurations
\$PMH/workload/spark/scripts/sweeping_spark.pl \$PMH/workload/spark/test_case/$script_dir $test_plan_fn $step->{"SWEEPING"} \$RUNDIR \$PMH

EOF
        close $script_fh;
        open $script_fh, "> $script_dir/_run_$step->{SWEEPING}.sh" or die "Cannot open file ".$script_dir."/_run_$step->{SWEEPING}.sh for write";

        # Print usage etc.
        my $sweeping_key_cnt = keys %{$step->{"SWEEPING_PARAMS"}};
        $sweeping_key_cnt = $sweeping_key_cnt + 1;
        my $key_str = "";
        my $idx = 2;
        $tag = $step->{"SWEEPING"};
        foreach my $key (keys %{$step->{"SWEEPING_PARAMS"}}) {
            $key_str = $key_str." <".$key.">";
            $sweeping_params = $sweeping_params." ".$key."\$$idx";
            $tag = $tag."-"."\$$idx";
            $idx = $idx + 1;
        }
        print $script_fh <<EOF;
#!/bin/bash
# This script is generated by generate_scripts.pl. Please do not edit directly!
if [ \$# -ne $sweeping_key_cnt ]
then
    echo "Usage: ./_run.sh RUNDIR $key_str\n";
    exit 1;
fi

RUNDIR=\$1
export SPARK_HOME=$spark_conf->{"SPARK_HOME"}
export HADOOP_HOME=$spark_conf->{"HADOOP_HOME"}
export HADOOP_CONF_DIR=$spark_conf->{"HADOOP_HOME"}/etc/hadoop
export PMH=$pmh
export WORKLOAD_NAME=$script_dir
export DESCRIPTION="$script_dir"
export WORKLOAD_DIR="."      # The workload working directory
export MEAS_DELAY_SEC=1      # Delay between each measurement
export MASTER=$spark_conf->{"MASTER"}
INFO=\$PMH/workload/spark/test_case/$script_dir/info
APPID=\$PMH/workload/spark/test_case/$script_dir/appid
DEBUG=\$PMH/workload/spark/test_case/$script_dir/debug.log

# SLAVES config required by run-workload.sh
unset SLAVES
SLAVES="$all_slaves"
export SLAVES

EOF
        $step->{"TAG"} = $step->{"SWEEPING"};
    } elsif (exists $step->{"TAG"}) {
        $tag = $step->{"TAG"};
    }

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
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh -l root {} "sync && echo 3 > /proc/sys/vm/drop_caches"

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
        }

        my $cmd = "";
        if ($step->{"CMD"}->{"COMMAND"} =~ /\<SPARK_HOME\>/) {
            $step->{"CMD"}->{"COMMAND"} =~ s/\<SPARK_HOME\>/$spark_conf->{"SPARK_HOME"}/;
        }
        if (not ((exists $step->{"COLLECT_NMON"}) and ($step->{"COLLECT_NMON"} eq "FALSE"))) {
            print $script_fh <<EOF;
# Stop/Start nmon
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ps -ef | grep nmon | grep -v grep | awk '{print \\\$2}' | xargs -i kill -9 \\{\\}"
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "nmon -f -s 5 -c 50000"
EOF
        }
        print $script_fh <<EOF;
CMD_TO_KILL="$step->{"CMD"}->{"COMMAND"}"
EOF
        $cmd = $cmd.$step->{"CMD"}->{"COMMAND"};
        my $def_conf = 0;
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

        # Substitute for SWEEPING block
        if ($sweeping == 1) {
            $cmd =~ s/SWEEPING_PARAMS/$sweeping_params/g; 
        }

        if ($sweeping == 0) {
            print $script_fh <<EOF;
echo \"TAG:$step->{"TAG"} COUNT:$repeat\" >> \$INFO
EOF
        }
        print $script_fh <<EOF;
for ITER in \$(seq $repeat)
do
EOF
        if ($drop_cache_between_run == 1) {
            print $script_fh <<EOF;
    if [ \$ITER -ne 1 ] 
    then
        sync && echo 3 > /proc/sys/vm/drop_caches
        grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh -l root {} "sync && echo 3 > /proc/sys/vm/drop_caches"
    fi
EOF
        }
        print $script_fh <<EOF;
    export RUN_ID=\"$tag-ITER\$ITER\"
    CMD=\"${cmd}\"
    CMD=\"\${CMD} > \$PMH/workload/spark/test_case/$script_dir/$tag-ITER\$ITER.log 2>&1\"
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
    \$PMH/workload/spark/scripts/query_yarn_app_id.pl \$APPID \$INFO $tag \$ITER $spark_conf->{"HADOOP_HOME"} \$PMH/workload/spark/scripts \$DEBUG &
EOF
        }
        print $script_fh <<EOF;
    \${PMH}/run-workload.sh
    DURATION=`grep "Elapsed (wall clock) time" \$RUNDIR/data/raw/$tag-ITER\${ITER}_time_stdout.txt | awk -F"m:ss): " '{print \$2}' | awk -F: 'END { if (NF == 2) {sum=\$1*60+\$2} else {sum=\$1*3600+\$2*60+\$3} print sum}'`
    echo \"TAG:$tag ITER:\$ITER DURATION:\$DURATION\" >> \$INFO
EOF
            print $script_fh <<EOF;
    APP_ID_FROM_LOG=""
    grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$tag-ITER\$ITER.log > /dev/null 2>&1
    if [ \$? -eq 0 ]
    then
        APP_ID_FROM_LOG=`grep "EventLoggingListener: Logging events to" \$PMH/workload/spark/test_case/$script_dir/$tag-ITER\$ITER.log | awk -F"file:" '{print \$2}' | awk -F/ '{print \$NF}'`;
    else
        grep "Submitted application" \$PMH/workload/spark/test_case/$script_dir/$tag-ITER\$ITER.log > /dev/null 2>&1
        if [ \$? -eq 0 ]
        then
            APP_ID_FROM_LOG=`grep "Submitted application" \$PMH/workload/spark/test_case/$script_dir/$tag-ITER\$ITER.log | awk '{print \$NF}'`;
        fi
    fi

    if [ \$APP_ID_FROM_LOG != "" ]
    then
        grep "TAG:$tag ITER:\$ITER APPID:\$APP_ID_FROM_LOG" \$INFO > /dev/null 2>&1
        if [ \$? -ne 0 ]
        then
            sed -i "s/TAG:$tag ITER:\$ITER APPID:.*\\\$/TAG:$tag ITER:\$ITER APPID:\$APP_ID_FROM_LOG/g" \$INFO
        fi
    fi

    grep "TAG:$tag ITER:\$ITER APPID:" \$INFO > /dev/null 2>&1
    while [ \$? -ne 0 ]
    do
        sleep 1
        grep "TAG:$tag ITER:\$ITER APPID:" \$INFO > /dev/null 2>&1
    done
    grep "TAG:$tag ITER:\$ITER APPID:TIMEOUT" \$INFO > /dev/null 2>&1
    if [ \$? -ne 0 ]
    then
        DST_EVENT_LOG_FN=`grep "TAG:$tag ITER:\$ITER APPID:" \$INFO | awk -F\"APPID:\" '{print \$2}'`;
        for SLAVE in \$SLAVES
        do
            scp \$SLAVE:$current_spark_event_dir/\$DST_EVENT_LOG_FN \$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$tag-ITER\$ITER > /dev/null 2>&1
        done
        scp $spark_conf->{"MASTER"}:$current_spark_event_dir/\$DST_EVENT_LOG_FN \$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$tag-ITER\$ITER > /dev/null 2>&1
        echo \"TAG:$tag ITER:\$ITER EVENTLOG:\$RUNDIR/spark_events/\${DST_EVENT_LOG_FN}-$tag-ITER\$ITER\" >> \$INFO
        $spark_conf->{"HADOOP_HOME"}/bin/yarn application -appStates FINISHED -list 2>&1 | grep \$DST_EVENT_LOG_FN > /dev/null 2>&1
        if [ \$? -eq 0 ]
        then
            echo \"TAG:$tag ITER:\$ITER STATUS:0\" >> \$INFO
        else
            echo \"TAG:$tag ITER:\$ITER STATUS:1\" >> \$INFO
        fi
        # FIXME: put time result into INFO
    else
        echo "Application ID not found for TAG:$tag ITER:\$ITER"
    fi
EOF
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
        if (not ((exists $step->{"COLLECT_NMON"}) and ($step->{"COLLECT_NMON"} eq "FALSE"))) {
            print $script_fh <<EOF;
# Stop nmon and collect nmon logs
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ps -ef | grep nmon | grep -v grep | awk '{print \\\$2}' | xargs -i kill -9 \\{\\}"
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ls -lrt *.nmon | tail -n 1 | awk '{print \\\$9}' | xargs -i scp \\{\\} $spark_conf->{"MASTER"}:\$RUNDIR/nmon/"

EOF
        }

    } elsif (exists $step->{"SHELL"}) {
        if ($step->{"SHELL"} =~ /\<HADOOP_HOME\>/) {
            $step->{"SHELL"} =~ s/\<HADOOP_HOME\>/$spark_conf->{"HADOOP_HOME"}/;
        }
        if ($step->{"SHELL"} =~ /\<SPARK_HOME\>/) {
            $step->{"SHELL"} =~ s/\<SPARK_HOME\>/$spark_conf->{"SPARK_HOME"}/;
        }
        if ($step->{"SHELL"} =~ /\<RUN_HOME\>/) {
            $step->{"SHELL"} =~ s/\<RUN_HOME\>/\$PMH\/workload\/spark\/test_case\/$script_dir/;
        }
        print $script_fh <<EOF;
# SHELL command
$step->{"SHELL"}
cd \$PMH

EOF
    } elsif (exists $step->{"BATCH"}) {
        print $script_fh <<EOF;
## Batch script ##
EOF

        if (not ((exists $step->{"COLLECT_NMON"}) and ($step->{"COLLECT_NMON"} eq "FALSE"))) {
            print $script_fh <<EOF;
# Stop/Start nmon
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ps -ef | grep nmon | grep -v grep | awk '{print \\\$2}' | xargs -i kill -9 \\{\\}"
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "nmon -f -s 5 -c 50000"

EOF
        }
        if ($step->{"CMD"} =~ /\<HADOOP_HOME\>/) {
            $step->{"CMD"} =~ s/\<HADOOP_HOME\>/$spark_conf->{"HADOOP_HOME"}/;
        }
        if ($step->{"CMD"} =~ /\<SPARK_HOME\>/) {
            $step->{"CMD"} =~ s/\<SPARK_HOME\>/$spark_conf->{"SPARK_HOME"}/;
        }
        if ($step->{"CMD"} =~ /\<RUN_HOME\>/) {
            $step->{"CMD"} =~ s/\<RUN_HOME\>/\$PMH\/workload\/spark\/test_case\/$script_dir/;
        }

        print $script_fh <<EOF;
export RUN_ID=\"$step->{"BATCH"}\"
CMD=\"$step->{"CMD"}\"
CMD=\"\${CMD} > \$PMH/workload/spark/test_case/$script_dir/$step->{"BATCH"}.log 2>&1\"
export WORKLOAD_CMD=\${CMD}
\${PMH}/run-workload.sh

EOF

        if (not ((exists $step->{"COLLECT_NMON"}) and ($step->{"COLLECT_NMON"} eq "FALSE"))) {
            print $script_fh <<EOF;
# Stop nmon and collect nmon logs
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ps -ef | grep nmon | grep -v grep | awk '{print \\\$2}' | xargs -i kill -9 \\{\\}"
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ls -lrt *.nmon | tail -n 1 | awk '{print \\\$9}' | xargs -i scp \\{\\} $spark_conf->{"MASTER"}:\$RUNDIR/nmon/"

EOF
        }
    } elsif (exists $step->{"HADOOP"}) {
        my $tag_idx = 0;
        if (exists $tags{$step->{"HADOOP"}}) {
            $tags{$step->{"HADOOP"}} = $tags{$step->{"HADOOP"}} + 1;
            $step->{"HADOOP"} = $step->{"HADOOP"}."_".$tags{$step->{"HADOOP"}};
        } else {
            $tags{$step->{"HADOOP"}} = 1;
        }
        if (not (exists $step->{"CMD"})) {
            close $script_fh;
            `rm -rf $script_dir_full`;
            die "Please define CMD section in HADOOP ".$step->{"HADOOP"};
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

        my $cmd = "";
        if ($step->{"CMD"}->{"COMMAND"} =~ /\<HADOOP_HOME\>/) {
            $step->{"CMD"}->{"COMMAND"} =~ s/\<HADOOP_HOME\>/$spark_conf->{"HADOOP_HOME"}/;
        }
        if (not ((exists $step->{"COLLECT_NMON"}) and ($step->{"COLLECT_NMON"} eq "FALSE"))) {
            print $script_fh <<EOF;
# Stop/Start nmon
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ps -ef | grep nmon | grep -v grep | awk '{print \\\$2}' | xargs -i kill -9 \\{\\}"
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "nmon -f -s 5 -c 50000"
EOF
        }
        print $script_fh <<EOF;
CMD_TO_KILL="$step->{"CMD"}->{"COMMAND"}"
EOF
        $cmd = $cmd.$step->{"CMD"}->{"COMMAND"};
        my $def_conf = 0;
        if (exists $step->{"CMD"}->{"PARAM"}) {
            foreach my $element (@{$step->{"CMD"}->{"PARAM"}}) {
                if ($element =~ /\<HADOOP_HOME\>/) {
                    $element =~ s/\<HADOOP_HOME\>/$spark_conf->{"HADOOP_HOME"}/;
                }
                if ($element =~ /\<HADOOP_MASTER_IP\>/) {
                    $element =~ s/\<HADOOP_MASTER_IP\>/$master_ip/;
                }
                $cmd = $cmd." ".$element;
            }
        }

        print $script_fh <<EOF;
echo \"TAG:$step->{"HADOOP"} COUNT:$repeat\" >> \$INFO
for ITER in \$(seq $repeat)
do
EOF
        if ($drop_cache_between_run == 1) {
            print $script_fh <<EOF;
    if [ \$ITER -ne 1 ] 
    then
        sync && echo 3 > /proc/sys/vm/drop_caches
        grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh -l root {} "sync && echo 3 > /proc/sys/vm/drop_caches"
    fi
EOF
        }
        print $script_fh <<EOF;
    export RUN_ID=\"$step->{"HADOOP"}-ITER\$ITER\"
    CMD=\"${cmd}\"
    CMD=\"\${CMD} > \$PMH/workload/spark/test_case/$script_dir/$step->{"HADOOP"}-ITER\$ITER.log 2>&1\"
    export WORKLOAD_CMD=\${CMD}
EOF
        # For YARN scheduler, get the latest FINISHED/FAILED/KILLED application-id
        if ($spark_conf->{"SCHEDULER"} eq "YARN") {
            print $script_fh <<EOF;
    # Get existing application-id infos
    echo "FINISHED" > \$APPID
    `\$PMH/workload/hive/scripts/query_yarn_app_id_in_some_state.pl $spark_conf->{"HADOOP_HOME"} FINISHED \$DEBUG >> \$APPID`;
    echo "FAILED" >> \$APPID
    `\$PMH/workload/hive/scripts/query_yarn_app_id_in_some_state.pl $spark_conf->{"HADOOP_HOME"} FAILED \$DEBUG >> \$APPID`;
    echo "KILLED" >> \$APPID
    `\$PMH/workload/hive/scripts/query_yarn_app_id_in_some_state.pl $spark_conf->{"HADOOP_HOME"} KILLED \$DEBUG >> \$APPID`;
    echo "RUNNING" >> \$APPID
    `\$PMH/workload/hive/scripts/query_yarn_app_id_in_some_state.pl $spark_conf->{"HADOOP_HOME"} RUNNING \$DEBUG >> \$APPID`;
    \$PMH/workload/spark/scripts/query_yarn_app_id.pl \$APPID \$INFO $step->{"HADOOP"} \$ITER $spark_conf->{"HADOOP_HOME"} \$PMH/workload/hive/scripts \$DEBUG &
EOF
        }
        print $script_fh <<EOF;
    \${PMH}/run-workload.sh
    DURATION=`grep "Elapsed (wall clock) time" \$RUNDIR/data/raw/$step->{"HADOOP"}-ITER\${ITER}_time_stdout.txt | awk -F"m:ss): " '{print \$2}' | awk -F: 'END { if (NF == 2) {sum=\$1*60+\$2} else {sum=\$1*3600+\$2*60+\$3} print sum}'`
    echo \"TAG:$step->{"HADOOP"} ITER:\$ITER DURATION:\$DURATION\" >> \$INFO
EOF
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
        if (not ((exists $step->{"COLLECT_NMON"}) and ($step->{"COLLECT_NMON"} eq "FALSE"))) {
            print $script_fh <<EOF;
# Stop nmon and collect nmon logs
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ps -ef | grep nmon | grep -v grep | awk '{print \\\$2}' | xargs -i kill -9 \\{\\}"
grep -v \\# $spark_conf->{"HADOOP_HOME"}/etc/hadoop/slaves | xargs -i ssh {} "ls -lrt *.nmon | tail -n 1 | awk '{print \\\$9}' | xargs -i scp \\{\\} $spark_conf->{"MASTER"}:\$RUNDIR/nmon/"

EOF
        }
    }
    # Restore settings for SWEEPING
    if ($sweeping == 1) {
        $sweeping = 0;
        $sweeping_params = "";
        close $script_fh;
        `chmod +x $script_dir/_run_$step->{SWEEPING}.sh`;
        open $script_fh, ">> $script_dir/run.sh" or die "Cannot open file ".$script_dir."/run.sh for append";
    }
    $tag = "";
}

print $script_fh <<EOF;
\$PMH/workload/spark/scripts/create_summary_table.pl \$PMH/workload/spark/test_case/$test_plan_fn \$RUNDIR

EOF

close $script_fh;
`chmod +x $script_dir/run.sh`;
