#!/bin/bash

if [ $# -ne 6 ]
then
    echo "Usage: ./spark_user.sh <SPARK_HOME> <DB_JAR> <SCALA_PATH> <DB basename> <Number of sequential queries> <Log folder>"
    exit 1
fi
SPARK_HOME=$1
DB_JAR=$2
SCALA_PATH=$3
DB_BASENAME=$4
NUM_SEQ_REQ=$5
LOG_FOLDER=$6
MAX_SCRIPT_IDX=8

for i in `seq ${NUM_SEQ_REQ}`
do
    SCRIPT_SEQ=`shuf -i 1-${MAX_SCRIPT_IDX} -n 1`
    DB_IDX=`shuf -i 1-20 -n 1`
    export DB_NAME=${DB_BASENAME}_${DB_IDX}
    begin_time=`date +%s`

    sleep `shuf -i 1-60 -n 1`
    ${SPARK_HOME}/bin/spark-shell --master yarn --num-executors 1 --executor-cores 1 --executor-memory 2g --driver-memory 4g --conf spark.executor.extraJavaOptions="-XX:MaxPermSize=256m -server -XX:+UseMembar -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 -XX:SurvivorRatio=4 -XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+UnlockExperimentalVMOptions -XX:+UseCriticalCompilerThreadPriority" --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/sparkLogs --jars ${DB_JAR} -i ${SCALA_PATH}/spark${SCRIPT_SEQ}.scala > /dev/null 2>&1

    end_time=`date +%s`
    echo "$((end_time - begin_time))" >> ${LOG_FOLDER}/spark_job_e2e_sec.log
done
