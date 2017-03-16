#!/bin/bash

if [ $# -ne 4 ]
then
    echo "Usage: ./hive_user.sh <SQL path> <DB basename> <Number of sequential queries> <Log folder>"
    exit 1
fi
SQL_PATH=$1
DB_BASENAME=$2
NUM_SEQ_REQ=$3
LOG_FOLDER=$4
MAX_SCRIPT_IDX=8

for i in `seq ${NUM_SEQ_REQ}`
do
    SCRIPT_SEQ=`shuf -i 1-${MAX_SCRIPT_IDX} -n 1`
    DB_IDX=`shuf -i 1-10 -n 1`
    begin_time=`date +%s`

    sleep `shuf -i 1-90 -n 1`
    hive --database ${DB_BASENAME}_${DB_IDX} -f ${SQL_PATH}/hive${SCRIPT_SEQ}.sql > /dev/null 2>&1

    end_time=`date +%s`
    echo "$((end_time - begin_time))" >> ${LOG_FOLDER}/hive_job_e2e_sec.log
done
