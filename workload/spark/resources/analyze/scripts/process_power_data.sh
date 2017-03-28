#!/bin/bash

if [ $# -lt 4 ]
then
    echo "Usage: ./$0 <log_dir> <start date: e.g. 2017-03-26 00:00:00> <period in sec> <count>"
    exit 1
fi

DIR=$1
HOUR_TS=`TZ='Asia/Shanghai' date --date="$2" +%s`
PERIOD=$3
COUNT=$4

`rm -f $DIR/timestamps`
while [ $COUNT -gt 0 ]
do
    HOUR_STRING=`TZ='Asia/Shanghai' date -d @$HOUR_TS`
    echo "$HOUR_TS $HOUR_STRING" >> $DIR/timestamps
    COUNT=`expr $COUNT \- 1`
    HOUR_TS=`expr $HOUR_TS \+ $PERIOD`
done

rm -f $DIR/mr_start_ts $DIR/mr_duration $DIR/mr
cat $DIR/mr_taskinfo | grep 2017 | awk -F, '{print $2}' | xargs -i sh -c "TZ='Asia/Shanghai' date --date='{}' +%s" >> $DIR/mr_start_ts
cat $DIR/mr_taskinfo | grep 2017 | awk -F, '{print $4}' >> $DIR/mr_duration
paste $DIR/mr_start_ts $DIR/mr_duration | column -s $' ' -t > $DIR/mr

rm -f $DIR/spsql_start_ts $DIR/spsql_duration $DIR/scala
cat $DIR/spsql_taskinfo | grep 2017 | awk -F, '{print $2}' | xargs -i sh -c "TZ='Asia/Shanghai' date --date='{}' +%s" >> $DIR/spsql_start_ts
cat $DIR/spsql_taskinfo | grep 2017 | awk -F, '{print $4}' >> $DIR/spsql_duration
paste $DIR/spsql_start_ts $DIR/spsql_duration | column -s $' ' -t > $DIR/scala

rm -f $DIR/scala_start_ts $DIR/scala_duration $DIR/spsql
cat $DIR/scala_taskinfo | grep 2017 | awk -F, '{print $2}' | xargs -i sh -c "TZ='Asia/Shanghai' date --date='{}' +%s" >> $DIR/scala_start_ts
cat $DIR/scala_taskinfo | grep 2017 | awk -F, '{print $4}' >> $DIR/scala_duration
paste $DIR/scala_start_ts $DIR/scala_duration | column -s $' ' -t > $DIR/spsql

gzip -d $DIR/sdr_log*.gz > /dev/null 2>&1
rm -f $DIR/power_ts $DIR/power_value
grep "CST" $DIR/sdr_log* | grep == | awk '{print $2 " " $3 " " $4 " " $5 " " $6 " " $7}' | xargs -i sh -c "TZ='Asia/Shanghai' date --date='{}' +%s" >> $DIR/power_ts
grep "Total Power" $DIR/sdr_log* | awk '{print $10}' >> $DIR/power_value
paste $DIR/power_ts $DIR/power_value | column -s $' ' -t > $DIR/power

hadoop fs -rm -skipTrash /tmp/timestamps
hadoop fs -rm -skipTrash /tmp/mr
hadoop fs -rm -skipTrash /tmp/scala
hadoop fs -rm -skipTrash /tmp/spsql
hadoop fs -rm -skipTrash /tmp/power
hadoop fs -copyFromLocal $DIR/timestamps /tmp
hadoop fs -copyFromLocal $DIR/mr /tmp
hadoop fs -copyFromLocal $DIR/scala /tmp
hadoop fs -copyFromLocal $DIR/spsql /tmp
hadoop fs -copyFromLocal $DIR/power /tmp
