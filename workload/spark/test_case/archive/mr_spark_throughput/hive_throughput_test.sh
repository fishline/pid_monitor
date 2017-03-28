#!/bin/bash
# Master script to start hive/spark mixed workload to driver full usage of vcore resources on single 11core Power node

if [ "$#" -ne 6 ]
then
    echo "Usage: ./hive_spark_throughput_test.sh <PMH> <SPARK_HOME> <DB_JAR> <SCALA_PATH> <RUN_PATH> <REQ_NUM>"
    exit 1
fi

PMH=$1
SPARK_HOME=$2
DB_JAR=$3
SCALA_PATH=$4
FOLDER=$5
REQ_NUM=$6
SPARK_REQ_DELTA=`expr $REQ_NUM \/ 5`
SPARK_REQ=`expr $REQ_NUM \- $SPARK_REQ_DELTA`

for i in `seq 20`
do
    ${PMH}/workload/hive/scripts/hive_user.sh $SCALA_PATH sql_20g ${REQ_NUM} ${FOLDER} &
done

for i in `seq 15`
do
    ${PMH}/workload/hive/scripts/hive_user.sh $SCALA_PATH sql_70g ${REQ_NUM} ${FOLDER} &
done

# sleep for 2hours, after that kill jobs and exit
sleep 7200
ps -ef | grep hive_user | grep pid_monitor | awk '{print $2}' | xargs -i kill -9 {}
yarn application -list 2>&1 | grep "^application" | awk '{print $1}' | xargs -i yarn application -kill {}
jps | grep RunJar | awk '{print $1}' | xargs -i kill -9 {}

exit 0
