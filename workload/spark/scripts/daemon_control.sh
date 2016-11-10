#!/bin/bash

if [ $# -lt 3 ]
then
    echo "Usage: ./daemon_control.sh <TGT_HOST> <SPARK_HOME> <ACTION: START/STOP>"
    exit 1
fi

TGT_HOST=$1
SPARK_HOME=$2
ACTION=$3

if [ $ACTION != "START" ] && [ $ACTION != "start" ] && [ $ACTION != "STOP" ] && [ $ACTION != "stop" ]
then
    echo "ACTION should be START/STOP/start/stop"
    exit 1
fi

if [ $ACTION = "STOP" ] || [ $ACTION = "stop" ]
then
    ssh $TGT_HOST "cd $SPARK_HOME && ./sbin/stop-all.sh"
fi

if [ $ACTION = "START" ] || [ $ACTION = "start" ]
then
    ssh $TGT_HOST "cd $SPARK_HOME && ./sbin/start-all.sh"
fi
