#!/bin/bash

FOLDER=$1
mkdir $FOLDER
# Need restart yarn daemon, so that application_id start from 1
/home/hduser/hadoop/sbin/stop-yarn.sh > /dev/null 2>&1
/home/hduser/hadoop/sbin/start-yarn.sh > /dev/null 2>&1
echo "datanode2:" > $FOLDER/network_before_test
ssh datanode2 ifconfig enp1s0f1 >> $FOLDER/network_before_test
echo "datanode3:" >> $FOLDER/network_before_test
ssh datanode3 ifconfig enP2p1s0f1 >> $FOLDER/network_before_test
ssh datanode1 "sync && echo 3 > /proc/sys/vm/drop_caches"
ssh datanode2 "sync && echo 3 > /proc/sys/vm/drop_caches"
ssh datanode3 "sync && echo 3 > /proc/sys/vm/drop_caches"
#date > $FOLDER/begin_time
begin_time=`date +%s`

hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where1.sql > $FOLDER/tpcds_hive_where1.sql.log 2>&1 &
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1.sql > $FOLDER/tpcds_hive_nowhere1.sql.log 2>&1 &
#hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_order_by1.sql > $FOLDER/tpcds_hive_order_by1.sql.log 2>&1 &
#hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_join1.sql > $FOLDER/tpcds_hive_join1.sql.log 2>&1 &
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where2.sql > $FOLDER/tpcds_hive_where2.sql.log 2>&1 &
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2.sql > $FOLDER/tpcds_hive_nowhere2.sql.log 2>&1 &
#hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_order_by2.sql > $FOLDER/tpcds_hive_order_by2.sql.log 2>&1 &
#hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_join2.sql > $FOLDER/tpcds_hive_join2.sql.log 2>&1 &
#hive --database tpcds_bin_partitioned_orc_1000 -f query18.sql > $FOLDER/query18.sql.log 2>&1 &
hive --database tpcds_bin_partitioned_orc_1000 -f query19.sql > $FOLDER/query19.sql.log 2>&1 &
hive --database tpcds_bin_partitioned_orc_1000 -f query68.sql > $FOLDER/query68.sql.log 2>&1 &
hive --database tpcds_bin_partitioned_orc_1000 -f query94.sql > $FOLDER/query94.sql.log 2>&1 &
wait

#date > $FOLDER/end_time
end_time=`date +%s`
echo "datanode2:" > $FOLDER/network_after_test
ssh datanode2 ifconfig enp1s0f1 >> $FOLDER/network_after_test
echo "datanode3:" >> $FOLDER/network_after_test
ssh datanode3 ifconfig enP2p1s0f1 >> $FOLDER/network_after_test

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
echo "Power finished $POWER maps, x86 finished $X86 maps"
echo "Map throughput ratio Power/x86:"
echo "scale=2; $POWER/$X86" | bc
echo "elapse time: $((end_time - begin_time)) secs"
