#!/bin/bash

if [ -z ${PMH} ]
then
    echo "Please set pid_monitor_home in var PMH"
    exit 1
fi

if [ $# -lt 3 ]
then
    echo "Usage: ./collect_spark_event_log.sh <TGT_HOST> <LOG_DIR> <EVENT_LOG_TAG>"
    exit 1
fi

TGT_HOST=$1
LOG_DIR=$2
TAG=$3

OUTPUT=$(ssh $TGT_HOST "ls -lrt $LOG_DIR | tail -n 1")
FN=$(echo $OUTPUT | awk '{print $9}')
scp -C $TGT_HOST:$LOG_DIR/$FN ${PMH}/workload/spark/event_logs/$FN.$TAG
