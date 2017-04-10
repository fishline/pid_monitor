#!/bin/bash
# Master script to start hive/spark mixed workload to driver full usage of vcore resources on single 11core Power node

if [ "$#" -ne 8 ]
then
    echo "Usage: ./hive_spark_throughput_test.sh <PMH> <SPARK_HOME> <DB_JAR> <SCALA_PATH> <RUN_PATH> <REQ_NUM> <MAX_USER> <MAX_APPS>"
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
MAX_USER=$7
MAX_APPS=$8
APP_JAR="NA"

for i in `seq 10`
do
    ${PMH}/workload/hive/scripts/hive_multi_user.sh $SCALA_PATH sql_20g ${REQ_NUM} ${FOLDER} ${MAX_USER} &
done

for i in `seq 10`
do
    ${PMH}/workload/hive/scripts/hive_multi_user.sh $SCALA_PATH sql_70g ${REQ_NUM} ${FOLDER} ${MAX_USER} &
done

for i in `seq 10`
do
    ${PMH}/workload/hive/scripts/hive_multi_user.sh $SCALA_PATH sql_140g ${REQ_NUM} ${FOLDER} ${MAX_USER} &
done

for i in `seq 5`
do
    ${PMH}/workload/spark/scripts/spark_sql_app_multi_user.sh $SPARK_HOME $DB_JAR $APP_JAR sql_20g ${REQ_NUM} ${FOLDER} 1 2g ${MAX_USER} ${MAX_APPS} &
done

for i in `seq 5`
do
    ${PMH}/workload/spark/scripts/spark_sql_app_multi_user.sh $SPARK_HOME $DB_JAR $APP_JAR sql_140g ${REQ_NUM} ${FOLDER} 1 4g ${MAX_USER} ${MAX_APPS} &
done

sleep 5400
ps -ef | grep hive_spark | awk '{print $2}' | xargs -i kill -9 {}
ps -ef | grep hive_multi | awk '{print $2}' | xargs -i kill -9 {}
ps -ef | grep spark_sql | awk '{print $2}' | xargs -i kill -9 {}
ps -ef | grep "master hive" | awk '{print $2}' | xargs -i kill -9 {}

ssh -l root master "ps -ef | grep \"hive --database\" | awk '{print \$2}' | xargs -i kill -9 {}"
ssh -l root master "ps -ef | grep \"hive\" | awk '{print \$2}' | xargs -i kill -9 {}"
ssh -l root master "ps -ef | grep \"spark-submit\" | awk '{print \$2}' | xargs -i kill -9 {}"
ssh -l root master "ps -ef | grep SparkQuery | awk '{print \$2}' | xargs -i kill -9 {}"

exit 0
