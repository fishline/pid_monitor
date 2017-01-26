#!/bin/bash

if [ $# -ne 2 ]
then
    echo "Usage: ./hive_user.sh <DB basename> <Number of sequential queries>"
    exit 1
fi
DB_BASENAME=$1
NUM_SEQ_REQ=$2
MAX_SCRIPT_IDX=8

for i in `seq ${NUM_SEQ_REQ}`
do
    SEQ=`shuf -i 1-${MAX_SCRIPT_IDX} -n 1`
    DB_IDX=`shuf -i 1-10 -n 1`
    hive --database ${DB_BASENAME}_${DB_IDX} -f hive${SEQ}.sql > /dev/null 2>&1
done
