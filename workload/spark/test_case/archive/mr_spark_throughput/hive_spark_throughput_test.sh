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

for i in `seq 15`
do
    ${PMH}/workload/hive/scripts/hive_user.sh $SCALA_PATH sql_20g ${REQ_NUM} ${FOLDER} &
done

for i in `seq 10`
do
    ${PMH}/workload/hive/scripts/hive_user.sh $SCALA_PATH sql_70g ${REQ_NUM} ${FOLDER} &
done

for i in `seq 10`
do
    ${PMH}/workload/spark/scripts/spark_user.sh $SPARK_HOME $DB_JAR $SCALA_PATH sql_20g ${REQ_NUM} ${FOLDER} &
done

wait

exit 0
