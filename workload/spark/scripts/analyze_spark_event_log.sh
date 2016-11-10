#!/bin/bash

if [ -z ${PMH} ]
then
    echo "Please set pid_monitor_home in var PMH"
    exit 1
fi

if [ $# -lt 1 ]
then
    echo "Usage: ./analyze_spark_event_log.sh <EVENT_LOG_TAG>"
    exit 1
fi

TAG=$1

RET=0
grep "Job Result" ${PMH}/workload/spark/event_logs/app-*.$TAG | grep JobSucceeded > /dev/null 2>&1
if [ $? -ne 0 ]
then
    RET=1
fi

START_TS=`grep "SparkListenerApplicationStart" ${PMH}/workload/spark/event_logs/app-*.$TAG | awk -FTimestamp\": '{print $2}' | awk -F, '{print $1}'`
STOP_TS=`grep "SparkListenerApplicationEnd" ${PMH}/workload/spark/event_logs/app-*.$TAG | awk -FTimestamp\": '{print $2}' | awk -F} '{print $1}'`
DELTA=`expr $STOP_TS \- $START_TS`
DELTA_S=`expr $DELTA \/ 1000`
echo $DELTA_S
exit $RET
