#!/bin/bash

if [ "$#" -ne 1 ]
then
    echo "Usage: ./test_hive_spark_throughput.sh <NAME of result folder>"
    exit 1
fi

FOLDER=RESULTS/$1

mkdir $FOLDER
if [ $? -ne 0 ]
then
    echo "Failed mkdir $FOLDER"
    exit 1
fi

# Need restart yarn daemon, we will have a clean picture for log analysis
/home/hduser/hadoop/sbin/stop-yarn.sh > /dev/null 2>&1
/home/hduser/hadoop/sbin/start-yarn.sh > /dev/null 2>&1
sleep 5
ssh datanode1 "sync && echo 3 > /proc/sys/vm/drop_caches"
ssh datanode2 "sync && echo 3 > /proc/sys/vm/drop_caches"
ssh datanode3 "sync && echo 3 > /proc/sys/vm/drop_caches"
ssh datanode1 "ps -ef | grep nmon | grep -v grep | awk '{print \$2}' | xargs -i kill -9 {}"
ssh datanode2 "ps -ef | grep nmon | grep -v grep | awk '{print \$2}' | xargs -i kill -9 {}"
ssh datanode3 "ps -ef | grep nmon | grep -v grep | awk '{print \$2}' | xargs -i kill -9 {}"
ssh datanode1 "nmon -f -s 5 -c 10000"
ssh datanode2 "nmon -f -s 5 -c 10000"
ssh datanode3 "nmon -f -s 5 -c 10000"
begin_time=`date +%s`

# Start hive queries first
# Notice: we have to make sure we have 3 queues of hive query running, and each queue has one running job
# spark run depends on this assumption

echo "Start spark now"
cd /home/felix/pid_monitor/workload/spark/test_case/spark_sql_run1-20170105031617 && ./run.sh > /home/felix/pid_monitor/workload/spark/test_case/spark_sql_run1-20170105031617/run.log 2>&1 &
cd /home/felix/pid_monitor/workload/spark/test_case/spark_sql_run2-20170105031627 && ./run.sh > /home/felix/pid_monitor/workload/spark/test_case/spark_sql_run2-20170105031627/run.log 2>&1 &
cd /home/felix/pid_monitor/workload/spark/test_case/spark_sql_run3-20170105031633 && ./run.sh > /home/felix/pid_monitor/workload/spark/test_case/spark_sql_run3-20170105031633/run.log 2>&1 &
cd /home/hduser/throughput_test

echo "Started hive queries"
./queue1.sh $FOLDER &
./queue2.sh $FOLDER &
./queue3.sh $FOLDER &

wait

end_time=`date +%s`
echo "elapse time: $((end_time - begin_time)) secs" > ${FOLDER}/result.log
ssh datanode1 "ps -ef | grep nmon | grep -v grep | awk '{print \$2}' | xargs -i kill -9 {}"
ssh datanode2 "ps -ef | grep nmon | grep -v grep | awk '{print \$2}' | xargs -i kill -9 {}"
ssh datanode3 "ps -ef | grep nmon | grep -v grep | awk '{print \$2}' | xargs -i kill -9 {}"

ssh datanode1 "ls -lrt | tail -n 1 | awk '{print \$9}' | xargs -i scp {} master:/home/hduser/throughput_test/$FOLDER"
ssh datanode2 "ls -lrt | tail -n 1 | awk '{print \$9}' | xargs -i scp {} master:/home/hduser/throughput_test/$FOLDER"
ssh datanode3 "ls -lrt | tail -n 1 | awk '{print \$9}' | xargs -i scp {} master:/home/hduser/throughput_test/$FOLDER"

# Collect jobhistory, spark event log
/home/felix/pid_monitor/workload/hive/scripts/query_yarn_app_id_in_some_state.pl /home/hduser/hadoop FINISHED | sed 's/application/job/g' | xargs -i ./wget_mapreduce_job_history.pl {} $FOLDER
cp /home/felix/pid_monitor/workload/spark/test_case/spark_sql_run1-20170105031617/rundir/*/latest/spark_events/* $FOLDER/
cp /home/felix/pid_monitor/workload/spark/test_case/spark_sql_run2-20170105031627/rundir/*/latest/spark_events/* $FOLDER/
cp /home/felix/pid_monitor/workload/spark/test_case/spark_sql_run3-20170105031633/rundir/*/latest/spark_events/* $FOLDER/

# Dump hive data
rm -f ${FOLDER}/stats.log
ls ${FOLDER} | grep job_ | xargs -i ./mapreduce_statistics_hadoop220.pl ${FOLDER}/{} >> ${FOLDER}/stats.log

echo "hive MAP_COUNT:" >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode1 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode1 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode2 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode2 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode3 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode3 " sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "hive simple queue1 MAP_COUNT:" >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log | grep QUEUE:root.queue1 | grep datanode1 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode1 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log | grep QUEUE:root.queue1 | grep datanode2 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode2 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log | grep QUEUE:root.queue1 | grep datanode3 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode3 " sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "hive simple queue2 MAP_COUNT:" >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log | grep QUEUE:root.queue2 | grep datanode1 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode1 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log | grep QUEUE:root.queue2 | grep datanode2 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode2 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log | grep QUEUE:root.queue2 | grep datanode3 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode3 " sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "hive complex queue MAP_COUNT:" >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log | grep QUEUE:default | grep datanode1 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode1 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log | grep QUEUE:default | grep datanode2 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode2 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log | grep QUEUE:default | grep datanode3 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode3 " sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "hive REDUCE_COUNT:" >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode1 | grep RED_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode1 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode2 | grep RED_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode2 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode3 | grep RED_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode3 " sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "hive MAP_HIVE_RECORDS_IN:" >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode1 | grep MAP_HIVE_RECORDS_IN | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode1 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode2 | grep MAP_HIVE_RECORDS_IN | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode2 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode3 | grep MAP_HIVE_RECORDS_IN | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode3 " sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "hive MAP_HDFS_BYTES_READ:" >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode1 | grep MAP_HDFS_BYTES_READ | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode1 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode2 | grep MAP_HDFS_BYTES_READ | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode2 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode3 | grep MAP_HDFS_BYTES_READ | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode3 " sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "spark task count:" >> ${FOLDER}/result.log
COUNT=`grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode1 | grep Success | wc -l`
echo "datanode1 $COUNT" >> ${FOLDER}/result.log
COUNT=`grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode2 | grep Success | wc -l`
echo "datanode2 $COUNT" >> ${FOLDER}/result.log
COUNT=`grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode3 | grep Success | wc -l`
echo "datanode3 $COUNT" >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "spark bytes read:" >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode1 | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -F: 'BEGIN {sum=0;} {sum+=$2} END {print "datanode1 "sum}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode2 | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -F: 'BEGIN {sum=0;} {sum+=$2} END {print "datanode2 "sum}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode3 | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -F: 'BEGIN {sum=0;} {sum+=$2} END {print "datanode3 "sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "spark records read:" >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode1 | grep Success | grep "Records Read" | awk -F"Records Read" '{print $2}' | awk -F\} '{print $1}' | awk -F: 'BEGIN {sum=0;} {sum+=$2} END {print "datanode1 "sum}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode2 | grep Success | grep "Records Read" | awk -F"Records Read" '{print $2}' | awk -F\} '{print $1}' | awk -F: 'BEGIN {sum=0;} {sum+=$2} END {print "datanode2 "sum}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode3 | grep Success | grep "Records Read" | awk -F"Records Read" '{print $2}' | awk -F\} '{print $1}' | awk -F: 'BEGIN {sum=0;} {sum+=$2} END {print "datanode3 "sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "spark task avg launch-finish time(ms):" >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode1 | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Launch Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file1
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode1 | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Finish Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file2
paste file1 file2 | column -s $' ' -t | awk 'BEGIN{sum=0; count=0;} {delta=($2 - $1); sum+=delta; count+=1} END {if (count==0){ print "datanode1 0"} else {print "datanode1 " sum/count}}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode2 | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Launch Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file1
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode2 | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Finish Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file2
paste file1 file2 | column -s $' ' -t | awk 'BEGIN{sum=0; count=0;} {delta=($2 - $1); sum+=delta; count+=1} END {if (count==0){ print "datanode2 0"} else {print "datanode2 " sum/count}}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode3 | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Launch Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file1
grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep datanode3 | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Finish Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file2
paste file1 file2 | column -s $' ' -t | awk 'BEGIN{sum=0; count=0;} {delta=($2 - $1); sum+=delta; count+=1} END {if (count==0){ print "datanode3 0"} else {print "datanode3 " sum/count}}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log
rm -f file1
rm -f file2

echo "hive TOTAL_MAP_DURATION(ms):" >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode1 | grep TOTAL_MAP_DURATION | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode1 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode2 | grep TOTAL_MAP_DURATION | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode2 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode3 | grep TOTAL_MAP_DURATION | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode3 " sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "hive TOTAL_MAP_CPU_TIME(ms):" >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode1 | grep TOTAL_MAP_CPU_TIME | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode1 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode2 | grep TOTAL_MAP_CPU_TIME | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode2 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode3 | grep TOTAL_MAP_CPU_TIME | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode3 " sum}' >> ${FOLDER}/result.log

cat ${FOLDER}/result.log
echo ""
grep -E 'datanode1|datanode2|datanode3' ${FOLDER}/result.log | awk '{print $2}' | tr '\n' ,
echo ""
exit 0
