#!/bin/bash

if [ $# -ne 10 ]
then
    echo "Usage: ./spark_user.sh <SPARK_HOME> <DB_JAR> <App jar file> <DB basename> <Number of sequential queries> <Log folder> <EXEC_CORE> <EXEC_MEM> <MAX_USER_COUNT> <MAX_APPS>"
    exit 1
fi
SPARK_HOME=$1
DB_JAR=$2
APP_JAR=$3
DB_BASENAME=$4
NUM_SEQ_REQ=$5
LOG_FOLDER=$6
EXEC_CORE=$7
EXEC_MEM=$8
MAX_USER=$9
MAX_SCRIPT_IDX=${10}

for i in `seq ${NUM_SEQ_REQ}`
do
    SCRIPT_SEQ=`shuf -i 1-${MAX_SCRIPT_IDX} -n 1`
    DB_IDX=`shuf -i 1-20 -n 1`
    export DB_NAME=${DB_BASENAME}_${DB_IDX}
    begin_time=`date +%s`
    USER_IDX=`shuf -i 1-${MAX_USER} -n 1`
    RATIO=`shuf -i 1-100 -n 1`
    if [ $RATIO -lt 70 ]
    then
        U=tdwadmin
    else
        U=test${USER_IDX}
    fi


    #sleep `shuf -i 1-60 -n 1`
    ssh -l ${U} master "/home/test_env/spark-1.6.1-bin-hadoop2.3/bin/spark-submit --master yarn --num-executors 1 --executor-cores ${EXEC_CORE} --executor-memory ${EXEC_MEM} --driver-memory 4g --conf spark.executor.extraJavaOptions=\"-XX:MaxPermSize=256m -server -XX:+UseMembar -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 -XX:SurvivorRatio=4 -XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+UnlockExperimentalVMOptions -XX:+UseCriticalCompilerThreadPriority\" --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/sparkLogs --jars /home/test_env/db-derby-10.11.1.1-bin/lib/derbyclient.jar --class src.main.scala.SparkQuery${SCRIPT_SEQ} /home/test_env/pid_monitor/workload/spark/resources/sqlgen/target/scala-2.10/sqlgen-app_2.10-1.0.jar ${DB_NAME} > /dev/null 2>&1"

    end_time=`date +%s`
    echo "$((end_time - begin_time))" >> ${LOG_FOLDER}/spark_job_e2e_sec.log
done
