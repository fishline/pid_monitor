#!/bin/bash

FOLDER=$1
mkdir $FOLDER
# Need restart yarn daemon, so that application_id start from 1
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

#./queue1.sh $FOLDER &
#./queue2.sh $FOLDER &
./queue3.sh $FOLDER &
wait

end_time=`date +%s`
ssh datanode1 "ps -ef | grep nmon | grep -v grep | awk '{print \$2}' | xargs -i kill -9 {}"
ssh datanode2 "ps -ef | grep nmon | grep -v grep | awk '{print \$2}' | xargs -i kill -9 {}"
ssh datanode3 "ps -ef | grep nmon | grep -v grep | awk '{print \$2}' | xargs -i kill -9 {}"

ssh datanode1 "ls -lrt | tail -n 1 | awk '{print \$9}' | xargs -i scp {} master:/home/hduser/throughput_test/$FOLDER"
ssh datanode2 "ls -lrt | tail -n 1 | awk '{print \$9}' | xargs -i scp {} master:/home/hduser/throughput_test/$FOLDER"
ssh datanode3 "ls -lrt | tail -n 1 | awk '{print \$9}' | xargs -i scp {} master:/home/hduser/throughput_test/$FOLDER"

JOB_HEAD=`grep "application" ./${FOLDER}/query19.sql.log | head -n 1 | awk '{print $4}' | awk -F, '{print $1}' | awk -F_ '{print $1"_"$2}'`

for i in `seq -f %02g 1 99`
do
    echo "${JOB_HEAD}_00$i" >> ./${FOLDER}/statistics.log
    ./get_per_node_task_duration_hadoop_220.pl ${JOB_HEAD}_00$i >> ./${FOLDER}/statistics.log
    if [ $? -ne 0 ]
    then
        break
    else
        mkdir ./${FOLDER}/${JOB_HEAD}_00${i}
        mv map ./${FOLDER}/${JOB_HEAD}_00${i}/map > /dev/null 2>&1
        mv reduce ./${FOLDER}/${JOB_HEAD}_00${i}/reduce > /dev/null 2>&1
    fi
done

POWER=`cat ${FOLDER}/statistics.log  | grep datanode3 | awk '{print $2}' | awk -F\( '{sum+=$2} END {print sum}'`
X86=`cat ${FOLDER}/statistics.log  | grep datanode2 | awk '{print $2}' | awk -F\( '{sum+=$2} END {print sum}'`
echo "Power finished $POWER maps, x86 finished $X86 maps" >> ./${FOLDER}/result.log
echo "Map throughput ratio Power/x86:" >> ./${FOLDER}/result.log
echo "scale=2; $POWER/$X86" | bc >> ./${FOLDER}/result.log

POWER=`cat ${FOLDER}/statistics.log | grep datanode3 | grep reduce | awk '{print $4}' | awk -F\( '{sum+=$2} END {print sum}'`
X86=`cat ${FOLDER}/statistics.log | grep datanode2 | grep reduce | awk '{print $4}' | awk -F\( '{sum+=$2} END {print sum}'`
echo "Power finished $POWER reduces, x86 finished $X86 reduces" >> ./${FOLDER}/result.log
echo "Reduce throughput ratio Power/x86:" >> ./${FOLDER}/result.log
echo "scale=2; $POWER/$X86" | bc >> ./${FOLDER}/result.log

echo "elapse time: $((end_time - begin_time)) secs" >> ./${FOLDER}/result.log
cat ./${FOLDER}/result.log
