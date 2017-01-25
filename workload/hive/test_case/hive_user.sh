#!/bin/bash

for i in `seq 20`
do
    SEQ=`shuf -i 1-4 -n 1`
    DB_IDX=`shuf -i 1-10 -n 1`
    hive --database sql_20g_${DB_IDX} -f os_order${SEQ}.sql
done
