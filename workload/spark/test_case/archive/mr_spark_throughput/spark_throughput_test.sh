#!/bin/bash
# Master script to start hive/spark mixed workload to driver full usage of vcore resources on single 11core Power node

if [ "$#" -ne 6 ]
then
    echo "Usage: ./hive_spark_throughput_test.sh <PMH> <SPARK_HOME> <DB_JAR> <APP_JAR> <RUN_PATH> <REQ_NUM>"
    exit 1
fi

PMH=$1
SPARK_HOME=$2
DB_JAR=$3
APP_JAR=$4
FOLDER=$5
REQ_NUM=$6

for i in `seq 10`
do
    ${PMH}/workload/spark/scripts/spark_sql_app.sh $SPARK_HOME $DB_JAR $APP_JAR sql_20g ${REQ_NUM} ${FOLDER} 1 2g &
done

for i in `seq 10`
do
    ${PMH}/workload/spark/scripts/spark_sql_app.sh $SPARK_HOME $DB_JAR $APP_JAR sql_70g ${REQ_NUM} ${FOLDER} 2 4g &
done

for i in `seq 10`
do
    ${PMH}/workload/spark/scripts/spark_sql_app.sh $SPARK_HOME $DB_JAR $APP_JAR sql_140g ${REQ_NUM} ${FOLDER} 4 8g &
done

# sleep for 2hours, after that kill jobs and exit
sleep 7200
ps -ef | grep spark_sql_app | grep pid_monitor | awk '{print $2}' | xargs -i kill -9 {}
yarn application -list 2>&1 | grep "^application" | awk '{print $1}' | xargs -i yarn application -kill {}
jps | grep SparkSubmit | awk '{print $1}' | xargs -i kill -9 {}

exit 0
