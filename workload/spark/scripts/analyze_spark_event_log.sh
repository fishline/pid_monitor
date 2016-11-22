#!/bin/bash

if [ $# -lt 1 ]
then
    echo "Usage: ./analyze_spark_event_log.sh <SPARK_EVENT_LOG>"
    exit 1
fi

FILE=$1

RET=0
grep "Job Result" $FILE | grep JobSucceeded > /dev/null 2>&1
if [ $? -ne 0 ]
then
    RET=1
fi

START_TS=`grep "SparkListenerApplicationStart" $FILE | awk -FTimestamp\": '{print $2}' | awk -F, '{print $1}'`
STOP_TS=`grep "SparkListenerApplicationEnd" $FILE | awk -FTimestamp\": '{print $2}' | awk -F} '{print $1}'`
DELTA=`expr $STOP_TS \- $START_TS`
DELTA_S=`expr $DELTA \/ 1000`
echo $DELTA_S
exit $RET
