#!/bin/bash

if [ $# -ne 3 ]
then
    echo "Usage: ./spark_user.sh <DB basename> <Number of sequential queries> <Log folder>"
    exit 1
fi
DB_BASENAME=$1
NUM_SEQ_REQ=$2
LOG_FOLDER=$3
MAX_SCRIPT_IDX=8

for i in `seq ${NUM_SEQ_REQ}`
do
    SCRIPT_SEQ=`shuf -i 1-${MAX_SCRIPT_IDX} -n 1`
    DB_IDX=`shuf -i 1-10 -n 1`
    export DB_NAME=${DB_BASENAME}_${DB_IDX}
    begin_time=`date +%s`

    /home/test/spark-1.6.1-bin-hadoop2.3/bin/spark-shell --master yarn --num-executors 1 --executor-cores 1 --executor-memory 2g --driver-memory 4g --conf spark.executor.extraJavaOptions="-XX:MaxPermSize=256m -server -XX:+UseMembar -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 -XX:SurvivorRatio=4 -XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+UnlockExperimentalVMOptions -XX:+UseCriticalCompilerThreadPriority" --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/sparkLogs --jars /home/maha/db-derby-10.11.1.1-bin/lib/derbyclient.jar -i /home/test/pid_monitor/workload/hive/test_case/spark${SCRIPT_SEQ}.scala

    end_time=`date +%s`
    echo "$((end_time - begin_time))" >> ${LOG_FOLDER}/spark_job_e2e_sec.log
done
