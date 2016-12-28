#!/bin/bash

FOLDER=$1

JOB_HEAD=`grep "application" ./${FOLDER}/query19.sql.log | head -n 1 | awk '{print $4}' | awk -F, '{print $1}' | awk -F_ '{print $1"_"$2}'`

for i in `seq -f %02g 1 21`
do
    echo "${JOB_HEAD}_00$i" >> ./${FOLDER}/statistics.log
    ./get_per_node_task_duration_hadoop_220.pl ${JOB_HEAD}_00$i >> ./${FOLDER}/statistics.log
    mkdir ./${FOLDER}/${JOB_HEAD}_00${i}
    mv map ./${FOLDER}/${JOB_HEAD}_00${i}/map
done

POWER=`cat ${FOLDER}/statistics.log  | grep datanode3 | awk '{print $2}' | awk -F\( '{sum+=$2} END {print sum}'`
X86=`cat ${FOLDER}/statistics.log  | grep datanode2 | awk '{print $2}' | awk -F\( '{sum+=$2} END {print sum}'`
echo "Map throughput ratio Power/x86:"
echo "scale=1; $POWER/$X86" | bc
