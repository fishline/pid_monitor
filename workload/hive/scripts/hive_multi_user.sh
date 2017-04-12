#!/bin/bash

if [ $# -ne 5 ]
then
    echo "Usage: ./hive_user.sh <SQL path> <DB basename> <Number of sequential queries> <Log folder>"
    exit 1
fi
SQL_PATH=$1
DB_BASENAME=$2
NUM_SEQ_REQ=$3
LOG_FOLDER=$4
MAX_USER=$5
MAX_SCRIPT_IDX=8

for i in `seq ${NUM_SEQ_REQ}`
do
    SCRIPT_SEQ=`shuf -i 1-${MAX_SCRIPT_IDX} -n 1`
    DB_IDX=`shuf -i 1-20 -n 1`
    begin_time=`date +%s`
    HEAP_SIZE=`shuf -e 1024 1664 2048 2816 8192 -n 1`
    USER_IDX=`shuf -i 1-${MAX_USER} -n 1`
    RATIO=`shuf -i 1-100 -n 1`
    if [ $RATIO -lt 70 ]
    then
        U=tdwadmin
    else
        U=test${USER_IDX}
    fi

    sleep `shuf -i 1-90 -n 1`
    ssh -l ${U} master "hive --database ${DB_BASENAME}_${DB_IDX} --hiveconf mapreduce.map.java.opts=\"-Xms${HEAP_SIZE}m -Xmx${HEAP_SIZE}m -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -XX:ConcGCThreads=2 -XX:+UseAdaptiveSizePolicy\" -f /home/test_env/pid_monitor/workload/spark/resources/sqlgen/scripts/hive${SCRIPT_SEQ}.sql > /dev/null 2>&1"

    end_time=`date +%s`
    echo "$((end_time - begin_time))" >> ${LOG_FOLDER}/hive_job_e2e_sec.log
done
