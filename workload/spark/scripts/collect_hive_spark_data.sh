#!/bin/bash

if [ "$#" -ne 2 ]
then
    echo "Usage: ./collect_hive_spark_data.sh <Start time: e.g. \"2017/03/13 10:30:00\"> <Stop time>"
    exit 1
fi

FIRST_HALF=`echo $1 | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
SECOND_HALF=`echo $2 | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
mkdir ./${FIRST_HALF}--${SECOND_HALF}

BEGIN_TIME=`date --date="$1" +"%s"`
END_TIME=`date --date="$2" +"%s"`

# Handle MR history
cd ./${FIRST_HALF}--${SECOND_HALF}
FOLDER=./
rm -f app
MASTER=`ip route show | grep ^default | awk '{print $5}' | xargs -i ifconfig {} | grep netmask | awk '{print $2}'`
wget http://${MASTER}:19888/jobhistory/app > /dev/null 2>&1
../../../../hive/scripts/filter_history.pl "$1" "$2" app | xargs -i ../../../../hive/scripts/wget_mapreduce_job_history.pl ${MASTER} 19888 {} $FOLDER
rm -f app

# Handle Spark history
PAGE=1
while [ $PAGE -lt 1000 ]
do
    rm -f index.html
    wget http://${MASTER}:18080/?page=${PAGE}\&showIncomplete=false -O index.html > /dev/null 2>&1
    ../../../../spark/scripts/filter_history.pl "$1" "$2" index.html | xargs -i \cp /tmp/sparkLogs/{} $FOLDER/
    PAGE=`expr $PAGE \+ 1`
    ts=`grep "sorttable_customkey" index.html | sed -n 2p | awk -F\" '{print \$2}'`
    ts=`expr $ts \/ 1000`
    if [ $ts -lt $BEGIN_TIME ]
    then
        break
    fi
done
rm -f index.html

DELTA=`expr $END_TIME \- $BEGIN_TIME`
echo "Test duration: $DELTA sec" > ${FOLDER}/result.log

# Dump hive data
rm -f ${FOLDER}/stats.log
ls ${FOLDER} | grep ^job_ | xargs -i ../../../../hive/scripts/mapreduce_statistics_hadoop220.pl ${FOLDER}/{} >> ${FOLDER}/stats.log

echo "hive MAP_COUNT:" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
do
    cat ${FOLDER}/stats.log  | grep $host | grep MAP_COUNT | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "hive REDUCE_COUNT:" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
do
    cat ${FOLDER}/stats.log  | grep $host | grep RED_COUNT | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "hive MAP_HIVE_RECORDS_IN:" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
do
    cat ${FOLDER}/stats.log  | grep $host | grep MAP_HIVE_RECORDS_IN | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "hive MAP_HDFS_BYTES_READ:" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
do
    cat ${FOLDER}/stats.log  | grep $host | grep MAP_HDFS_BYTES_READ | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "hive TOTAL_MAP_DURATION(ms):" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
do
    cat ${FOLDER}/stats.log  | grep $host | grep TOTAL_MAP_DURATION | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "hive TOTAL_MAP_CPU_TIME(ms):" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
do
    cat ${FOLDER}/stats.log  | grep $host | grep TOTAL_MAP_CPU_TIME | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "spark task count:" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
do
    COUNT=`grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep $host | grep Success | wc -l`
    echo "$host $COUNT" >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "spark bytes read:" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
do
    grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep $host | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -v var="$host" -F: 'BEGIN {sum=0;} {sum+=$2} END {print var " "sum}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "spark records read:" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
do
    grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep $host | grep Success | grep "Records Read" | awk -F"Records Read" '{print $2}' | awk -F\} '{print $1}' | awk -v var="$host" -F: 'BEGIN {sum=0;} {sum+=$2} END {print var " "sum}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "spark task avg launch-finish time(ms):" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
do
    grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep $host | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Launch Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file1
    grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep $host | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Finish Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file2
    paste file1 file2 | column -s $' ' -t | awk -v var="$host" 'BEGIN{sum=0; count=0;} {delta=($2 - $1); sum+=delta; count+=1} END {if (count==0){ print var " 0"} else {print var " " sum/count}}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
    rm -f file1
    rm -f file2
done

rm -rf ${FOLDER}/job_*
rm -rf ${FOLDER}/application_*
cd ../

exit 0
