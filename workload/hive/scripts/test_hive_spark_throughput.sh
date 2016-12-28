#!/bin/bash

if [ "$#" -ne 2 ]
then
    echo "Usage: ./test_hive_spark_throughput.sh <NAME of result folder> <\"CMS\" or \"Parallel\" for spark run config>"
    exit 1
fi

FOLDER=RESULTS/$1
SPARK_GC_TYPE=$2
CMS_SPARK_DIR=/home/felix/pid_monitor/workload/spark/test_case/cms_kmeans_with_hive-20161228023605
PARALLEL_SPARK_DIR=/home/felix/pid_monitor/workload/spark/test_case/parallel_kmeans_with_hive-20161228023614

mkdir $FOLDER
if [ $? -ne 0 ]
then
    echo "Failed mkdir $FOLDER"
    exit 1
fi
if [ "$SPARK_GC_TYPE" != "CMS" ] && [ "$SPARK_GC_TYPE" != "Parallel" ]
then
    echo "spark run config should be either \"CMS\" or \"Parallel\""
    exit 1
fi

# Need restart yarn daemon, we will have a clean picture for log analysis
/home/hduser/hadoop/sbin/stop-yarn.sh > /dev/null 2>&1
/home/hduser/hadoop/sbin/start-yarn.sh > /dev/null 2>&1
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
./queue1.sh $FOLDER &
./queue2.sh $FOLDER &
./queue3.sh $FOLDER &
echo "Started hive queries"
wait
exit 0

# Wait until there are 3 running jobs to start spark task
JOB_COUNT=`/home/felix/pid_monitor/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/hduser/hadoop RUNNING | grep -v NONE | wc -l | xargs echo -n`
while [ $JOB_COUNT -ne 3 ]
do
    sleep 1
    JOB_COUNT=`/home/felix/pid_monitor/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/hduser/hadoop RUNNING | grep -v NONE | wc -l | xargs echo -n`
done

echo "Start spark now"
SPARK_DIR=$CMS_SPARK_DIR
if [ "$SPARK_GC_TYPE" == "Parallel" ]
then
    SPARK_DIR=$PARALLEL_SPARK_DIR
fi
cd $SPARK_DIR && ./run.sh > $SPARK_DIR/run.log 2>&1 &
cd /home/hduser/throughput_test
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
/home/felix/pid_monitor/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/hduser/hadoop FINISHED | sed 's/application/job/g' | xargs -i ./wget_mapreduce_job_history.pl {} $FOLDER
cp ${SPARK_DIR}/rundir/*/latest/spark_events/* $FOLDER/

# Dump hive data
rm -f ${FOLDER}/stats.log
ls ${FOLDER} | grep job_ | xargs -i ./mapreduce_statistics_hadoop220.pl ${FOLDER}/{} >> ${FOLDER}/stats.log

echo "hive MAP_COUNT:" >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode1 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode1 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode2 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode2 " sum}' >> ${FOLDER}/result.log
cat ${FOLDER}/stats.log  | grep datanode3 | grep MAP_COUNT | awk '{print $2}' | awk -F: '{sum+=$2} END {print "datanode3 " sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "hive RED_COUNT:" >> ${FOLDER}/result.log
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
COUNT=`grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode1 | grep Success | wc -l`
echo "datanode1 $COUNT" >> ${FOLDER}/result.log
COUNT=`grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode2 | grep Success | wc -l`
echo "datanode2 $COUNT" >> ${FOLDER}/result.log
COUNT=`grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode3 | grep Success | wc -l`
echo "datanode3 $COUNT" >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "spark bytes read:" >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode1 | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -F: '{sum+=$2} END {print "datanode1 "sum}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode2 | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -F: '{sum+=$2} END {print "datanode2 "sum}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode3 | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -F: '{sum+=$2} END {print "datanode3 "sum}' >> ${FOLDER}/result.log
echo "" >> ${FOLDER}/result.log

echo "spark records read:" >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode1 | grep Success | grep "Records Read" | awk -F"Records Read" '{print $2}' | awk -F\} '{print $1}' | awk -F: '{sum+=$2} END {print "datanode1 "sum}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode2 | grep Success | grep "Records Read" | awk -F"Records Read" '{print $2}' | awk -F\} '{print $1}' | awk -F: '{sum+=$2} END {print "datanode2 "sum}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode3 | grep Success | grep "Records Read" | awk -F"Records Read" '{print $2}' | awk -F\} '{print $1}' | awk -F: '{sum+=$2} END {print "datanode3 "sum}' >> ${FOLDER}/result.log

MAX_SIZE1=`grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode1 | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' | sort -n -r | head -n 1`
MAX_SIZE2=`grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode2 | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' | sort -n -r | head -n 1`
MAX_SIZE3=`grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode3 | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' | sort -n -r | head -n 1`
echo "spark task avg GC time(ms):" >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode1 | grep Success | grep "Bytes Read\":${MAX_SIZE1}" | awk -F"JVM GC Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{sum+=$2; count+=1} END {print "datanode1 " sum/count}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode2 | grep Success | grep "Bytes Read\":${MAX_SIZE2}" | awk -F"JVM GC Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{sum+=$2; count+=1} END {print "datanode2 " sum/count}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode3 | grep Success | grep "Bytes Read\":${MAX_SIZE3}" | awk -F"JVM GC Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{sum+=$2; count+=1} END {print "datanode3 " sum/count}' >> ${FOLDER}/result.log
echo ""

echo "spark task avg execution time(ms):" >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode1 | grep Success | grep "Bytes Read\":${MAX_SIZE1}" | awk -F"Executor Run Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{sum+=$2; count+=1} END {print "datanode1 " sum/count}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode2 | grep Success | grep "Bytes Read\":${MAX_SIZE2}" | awk -F"Executor Run Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{sum+=$2; count+=1} END {print "datanode2 " sum/count}' >> ${FOLDER}/result.log
grep "SparkListenerTaskEnd" ${FOLDER}/application_*-ITER1 | grep datanode3 | grep Success | grep "Bytes Read\":${MAX_SIZE3}" | awk -F"Executor Run Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{sum+=$2; count+=1} END {print "datanode3 " sum/count}' >> ${FOLDER}/result.log

cat ${FOLDER}/result.log
exit 0
