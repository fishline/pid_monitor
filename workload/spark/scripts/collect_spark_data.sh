#!/bin/bash

if [ "$#" -ne 2 ]
then
    echo "Usage: ./collect_spark_data.sh <Start time: e.g. \"2017/03/13 10:30:00\"> <Stop time>"
    exit 1
fi

FIRST_HALF=`echo $1 | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
SECOND_HALF=`echo $2 | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
mkdir ./${FIRST_HALF}--${SECOND_HALF}--spark

BEGIN_TIME=`date --date="$1" +"%s"`
END_TIME=`date --date="$2" +"%s"`

# Handle MR history
cd ./${FIRST_HALF}--${SECOND_HALF}--spark
cp /home/hadoop-2.2.0/etc/hadoop/slaves ./
FOLDER=./
MASTER=`ip route show | grep ^default | awk '{print $5}' | xargs -i ifconfig {} | grep netmask | awk '{print $2}'`

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

echo "spark task count:" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/slaves`
do
    COUNT=`grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep $host | grep Success | wc -l`
    echo "$host $COUNT" >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "spark bytes read:" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/slaves`
do
    grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep $host | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -v var="$host" -F: 'BEGIN {sum=0;} {sum+=$2} END {print var " "sum}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "spark records read:" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/slaves`
do
    grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep $host | grep Success | grep "Records Read" | awk -F"Records Read" '{print $2}' | awk -F\} '{print $1}' | awk -v var="$host" -F: 'BEGIN {sum=0;} {sum+=$2} END {print var " "sum}' >> ${FOLDER}/result.log
    echo "" >> ${FOLDER}/result.log
done

echo "spark task avg launch-finish time(ms):" >> ${FOLDER}/result.log
for host in `cat ${FOLDER}/slaves`
do
    grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep $host | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Launch Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file1
    grep "SparkListenerTaskEnd" ${FOLDER}/application_* | grep $host | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Finish Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file2
    paste file1 file2 | column -s $' ' -t | awk -v var="$host" 'BEGIN{sum=0; count=0;} {delta=($2 - $1); sum+=delta; count+=1} END {if (count==0){ print var " 0"} else {print var " " sum/count}}' >> ${FOLDER}/result.log
    paste file1 file2 | column -s $' ' -t | awk -v var="$host" '{delta=($2 - $1); print delta}' >> ${FOLDER}/task_time.log
    echo "" >> ${FOLDER}/result.log
    rm -f file1
    rm -f file2
done

cd ../

exit 0
