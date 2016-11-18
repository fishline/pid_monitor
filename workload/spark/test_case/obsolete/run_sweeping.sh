#!/bin/bash

if [ -z ${PMH} ]
then
    echo "Please set pid_monitor_home in var PMH"
    exit 1
fi

if [ $# -lt 6 ]
then
    echo "Usage: run_sweeping.sh <TGT_HOST> <SPARK_HOME> <TEST_TAG> <RESULT_TAG> <RESTART_DAEMON> <UPDATE_CONFIG>"
    exit 1
fi

TGT_HOST=$1
SPARK_HOME=$2
TEST_TAG=$3
RESULT_TAG=$4
RESTART_DAEMON=$5
UPDATE_CONFIG=$6

if [ $UPDATE_CONFIG = "UC" ]
then
    RESTART_DAEMON="RD"
fi

# Stop Daemon
if [ $RESTART_DAEMON = "RD" ]
then
    ${PMH}/workload/spark/scripts/daemon_control.sh $TGT_HOST $SPARK_HOME STOP
    ${PMH}/workload/spark/scripts/os_clean_cache.sh $TGT_HOST $SPARK_HOME
fi

# Update config if required
if [ $UPDATE_CONFIG = "UC" ]
then
    ${PMH}/workload/spark/scripts/upload_spark_conf.sh $TGT_HOST $SPARK_HOME $TEST_TAG
fi

# Start Daemon
if [ $RESTART_DAEMON = "RD" ]
then
    ${PMH}/workload/spark/scripts/daemon_control.sh $TGT_HOST $SPARK_HOME START
fi

# Upload test case
scp ${PMH}/workload/spark/test_case/run.sh.$TEST_TAG $TGT_HOST:/tmp
ssh $TGT_HOST "chmod +x /tmp/run.sh.$TEST_TAG && cd /home/felix && /tmp/run.sh.$TEST_TAG"

# Collect logs
${PMH}/workload/spark/scripts/collect_spark_event_log.sh $TGT_HOST /tmp/sparkLogs $RESULT_TAG
