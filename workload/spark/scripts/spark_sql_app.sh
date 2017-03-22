#!/bin/bash

if [ $# -ne 8 ]
then
    echo "Usage: ./spark_user.sh <SPARK_HOME> <DB_JAR> <App jar file> <DB basename> <Number of sequential queries> <Log folder> <EXEC_CORE> <EXEC_MEM>"
    exit 1
fi
SPARK_HOME=$1
DB_JAR=$2
APP_JAR=$3
DB_BASENAME=$4
NUM_SEQ_REQ=$5
LOG_FOLDER=$6
MAX_SCRIPT_IDX=8
EXEC_CORE=$7
EXEC_MEM=$8

for i in `seq ${NUM_SEQ_REQ}`
do
    SCRIPT_SEQ=`shuf -i 1-${MAX_SCRIPT_IDX} -n 1`
    DB_IDX=`shuf -i 1-10 -n 1`
    export DB_NAME=${DB_BASENAME}_${DB_IDX}
    begin_time=`date +%s`

    sleep `shuf -i 1-60 -n 1`
    ${SPARK_HOME}/bin/spark-submit --master yarn --num-executors 1 --executor-cores ${EXEC_CORE} --executor-memory ${EXEC_MEM} --driver-memory 4g --conf spark.executor.extraJavaOptions="-XX:MaxPermSize=256m -server -XX:+UseMembar -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 -XX:SurvivorRatio=4 -XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+UnlockExperimentalVMOptions -XX:+UseCriticalCompilerThreadPriority" --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/sparkLogs --jars ${DB_JAR} --class src.main.scala.SparkQuery${SCRIPT_SEQ} ${APP_JAR} ${DB_NAME} > /dev/null 2>&1

    end_time=`date +%s`
    echo "$((end_time - begin_time))" >> ${LOG_FOLDER}/spark_job_e2e_sec.log
done
