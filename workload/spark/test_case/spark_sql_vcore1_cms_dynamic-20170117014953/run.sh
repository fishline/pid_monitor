#!/bin/bash
# This script is generated by generate_scripts.pl
ctrl_c_exit() {
    echo "Cleanup environment before exit now"
    if [ $CMD_TO_KILL != "" ]
    then
        echo "Got command to kill $CMD_TO_KILL"
        `ps -ef | grep "$CMD_TO_KILL" | grep -v grep | awk '{print $2}' | xargs -i kill -9 {}`
        `ps -ef | grep java | grep SparkSubmit | awk '{print $2}' | xargs -i kill -9 {}`
        $PMH/workload/spark/scripts/create_summary_table.pl $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic.json $RUNDIR

    fi
    exit 1
}

CMD_TO_KILL=""
SMT_NEED_RESET=0
trap ctrl_c_exit INT

export PMH=/home/felix/Github/pid_monitor
export WORKLOAD_NAME=spark_sql_vcore1_cms_dynamic-20170117014953
export DESCRIPTION="spark_sql_vcore1_cms_dynamic-20170117014953"
export WORKLOAD_DIR="."      # The workload working directory
export MEAS_DELAY_SEC=1      # Delay between each measurement
export RUNDIR=$(${PMH}/setup-run.sh $WORKLOAD_NAME)
mkdir $RUNDIR/spark_events
INFO=$PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/info
APPID=$PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/appid
DEBUG=$PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/debug.log
rm -f $INFO

# SLAVES config required by run-workload.sh
unset SLAVES
SLAVES="datanode3 datanode2"
export SLAVES

cd $PMH
cp -R html $RUNDIR/html

CMD_TO_KILL="/home/hduser/spark-1.6.1-bin-hadoop2.3/bin/spark-shell"
echo "TAG:spark-vcore1-cms-dyn1 COUNT:1" >> $INFO
for ITER in $(seq 1)
do
    export RUN_ID="spark-vcore1-cms-dyn1-ITER$ITER"
    CMD="/home/hduser/spark-1.6.1-bin-hadoop2.3/bin/spark-shell --master yarn --executor-cores 1 --executor-memory 1g --driver-memory 4g --conf spark.executor.extraJavaOptions=\"-XX:MaxPermSize=256m -server -XX:+UseMembar -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 -XX:SurvivorRatio=4 -XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=70\" --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/sparkLogs --queue root.spark_queue.vcore1 --jars /home/maha/db-derby-10.11.1.1-bin/lib/derbyclient.jar -i /home/felix/Github/pid_monitor/workload/hive/test_case/spark-vcore1-iter1.scala"
    CMD="${CMD} > $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn1-ITER$ITER.log 2>&1"
    export WORKLOAD_CMD=${CMD}
    # Get existing application-id infos
    echo "FINISHED" > $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 FINISHED $DEBUG >> $APPID`;
    echo "FAILED" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 FAILED $DEBUG >> $APPID`;
    echo "KILLED" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 KILLED $DEBUG >> $APPID`;
    echo "RUNNING" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 RUNNING $DEBUG >> $APPID`;
    $PMH/workload/spark/scripts/query_yarn_app_id.pl $APPID $INFO spark-vcore1-cms-dyn1 $ITER /home/maha/hadoop-2.2.0 $PMH/workload/spark/scripts $DEBUG &
    ${PMH}/run-workload.sh
    DURATION=`grep "Elapsed (wall clock) time" $RUNDIR/data/raw/spark-vcore1-cms-dyn1-ITER${ITER}_time_stdout.txt | awk -F"m:ss): " '{print $2}' | awk -F: 'END { if (NF == 2) {sum=$1*60+$2} else {sum=$1*3600+$2*60+$3} print sum}'`
    echo "TAG:spark-vcore1-cms-dyn1 ITER:$ITER DURATION:$DURATION" >> $INFO
    APP_ID_FROM_LOG=""
    grep "EventLoggingListener: Logging events to" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn1-ITER$ITER.log > /dev/null 2>&1
    if [ $? -eq 0 ]
    then
        APP_ID_FROM_LOG=`grep "EventLoggingListener: Logging events to" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn1-ITER$ITER.log | awk -F"file:" '{print $2}' | awk -F/ '{print $NF}'`;
    else
        grep "Submitted application" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn1-ITER$ITER.log > /dev/null 2>&1
        if [ $? -eq 0 ]
        then
            APP_ID_FROM_LOG=`grep "Submitted application" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn1-ITER$ITER.log | awk '{print $NF}'`;
        fi
    fi

    if [ $APP_ID_FROM_LOG != "" ]
    then
        grep "TAG:spark-vcore1-cms-dyn1 ITER:$ITER APPID:$APP_ID_FROM_LOG" $INFO > /dev/null 2>&1
        if [ $? -ne 0 ]
        then
            sed -i "s/TAG:spark-vcore1-cms-dyn1 ITER:$ITER APPID:.*\$/TAG:spark-vcore1-cms-dyn1 ITER:$ITER APPID:$APP_ID_FROM_LOG/g" $INFO
        fi
    fi

    grep "TAG:spark-vcore1-cms-dyn1 ITER:$ITER APPID:" $INFO > /dev/null 2>&1
    while [ $? -ne 0 ]
    do
        sleep 1
        grep "TAG:spark-vcore1-cms-dyn1 ITER:$ITER APPID:" $INFO > /dev/null 2>&1
    done
    grep "TAG:spark-vcore1-cms-dyn1 ITER:$ITER APPID:TIMEOUT" $INFO > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        DST_EVENT_LOG_FN=`grep "TAG:spark-vcore1-cms-dyn1 ITER:$ITER APPID:" $INFO | awk -F"APPID:" '{print $2}'`;
        for SLAVE in $SLAVES
        do
            scp $SLAVE:/tmp/sparkLogs/$DST_EVENT_LOG_FN $RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn1-ITER$ITER > /dev/null 2>&1
        done
        scp namenode:/tmp/sparkLogs/$DST_EVENT_LOG_FN $RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn1-ITER$ITER > /dev/null 2>&1
        echo "TAG:spark-vcore1-cms-dyn1 ITER:$ITER EVENTLOG:$RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn1-ITER$ITER" >> $INFO
        /home/maha/hadoop-2.2.0/bin/yarn application -appStates FINISHED -list 2>&1 | grep $DST_EVENT_LOG_FN > /dev/null 2>&1
        if [ $? -eq 0 ]
        then
            echo "TAG:spark-vcore1-cms-dyn1 ITER:$ITER STATUS:0" >> $INFO
        else
            echo "TAG:spark-vcore1-cms-dyn1 ITER:$ITER STATUS:1" >> $INFO
        fi
        # FIXME: put time result into INFO
    else
        echo "Application ID not found for TAG:spark-vcore1-cms-dyn1 ITER:$ITER"
    fi
done

CMD_TO_KILL="/home/hduser/spark-1.6.1-bin-hadoop2.3/bin/spark-shell"
echo "TAG:spark-vcore1-cms-dyn2 COUNT:1" >> $INFO
for ITER in $(seq 1)
do
    export RUN_ID="spark-vcore1-cms-dyn2-ITER$ITER"
    CMD="/home/hduser/spark-1.6.1-bin-hadoop2.3/bin/spark-shell --master yarn --executor-cores 1 --executor-memory 1g --driver-memory 4g --conf spark.executor.extraJavaOptions=\"-XX:MaxPermSize=256m -server -XX:+UseMembar -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 -XX:SurvivorRatio=4 -XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=70\" --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/sparkLogs --queue root.spark_queue.vcore1 --jars /home/maha/db-derby-10.11.1.1-bin/lib/derbyclient.jar -i /home/felix/Github/pid_monitor/workload/hive/test_case/spark-vcore1-iter2.scala"
    CMD="${CMD} > $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn2-ITER$ITER.log 2>&1"
    export WORKLOAD_CMD=${CMD}
    # Get existing application-id infos
    echo "FINISHED" > $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 FINISHED $DEBUG >> $APPID`;
    echo "FAILED" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 FAILED $DEBUG >> $APPID`;
    echo "KILLED" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 KILLED $DEBUG >> $APPID`;
    echo "RUNNING" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 RUNNING $DEBUG >> $APPID`;
    $PMH/workload/spark/scripts/query_yarn_app_id.pl $APPID $INFO spark-vcore1-cms-dyn2 $ITER /home/maha/hadoop-2.2.0 $PMH/workload/spark/scripts $DEBUG &
    ${PMH}/run-workload.sh
    DURATION=`grep "Elapsed (wall clock) time" $RUNDIR/data/raw/spark-vcore1-cms-dyn2-ITER${ITER}_time_stdout.txt | awk -F"m:ss): " '{print $2}' | awk -F: 'END { if (NF == 2) {sum=$1*60+$2} else {sum=$1*3600+$2*60+$3} print sum}'`
    echo "TAG:spark-vcore1-cms-dyn2 ITER:$ITER DURATION:$DURATION" >> $INFO
    APP_ID_FROM_LOG=""
    grep "EventLoggingListener: Logging events to" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn2-ITER$ITER.log > /dev/null 2>&1
    if [ $? -eq 0 ]
    then
        APP_ID_FROM_LOG=`grep "EventLoggingListener: Logging events to" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn2-ITER$ITER.log | awk -F"file:" '{print $2}' | awk -F/ '{print $NF}'`;
    else
        grep "Submitted application" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn2-ITER$ITER.log > /dev/null 2>&1
        if [ $? -eq 0 ]
        then
            APP_ID_FROM_LOG=`grep "Submitted application" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn2-ITER$ITER.log | awk '{print $NF}'`;
        fi
    fi

    if [ $APP_ID_FROM_LOG != "" ]
    then
        grep "TAG:spark-vcore1-cms-dyn2 ITER:$ITER APPID:$APP_ID_FROM_LOG" $INFO > /dev/null 2>&1
        if [ $? -ne 0 ]
        then
            sed -i "s/TAG:spark-vcore1-cms-dyn2 ITER:$ITER APPID:.*\$/TAG:spark-vcore1-cms-dyn2 ITER:$ITER APPID:$APP_ID_FROM_LOG/g" $INFO
        fi
    fi

    grep "TAG:spark-vcore1-cms-dyn2 ITER:$ITER APPID:" $INFO > /dev/null 2>&1
    while [ $? -ne 0 ]
    do
        sleep 1
        grep "TAG:spark-vcore1-cms-dyn2 ITER:$ITER APPID:" $INFO > /dev/null 2>&1
    done
    grep "TAG:spark-vcore1-cms-dyn2 ITER:$ITER APPID:TIMEOUT" $INFO > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        DST_EVENT_LOG_FN=`grep "TAG:spark-vcore1-cms-dyn2 ITER:$ITER APPID:" $INFO | awk -F"APPID:" '{print $2}'`;
        for SLAVE in $SLAVES
        do
            scp $SLAVE:/tmp/sparkLogs/$DST_EVENT_LOG_FN $RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn2-ITER$ITER > /dev/null 2>&1
        done
        scp namenode:/tmp/sparkLogs/$DST_EVENT_LOG_FN $RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn2-ITER$ITER > /dev/null 2>&1
        echo "TAG:spark-vcore1-cms-dyn2 ITER:$ITER EVENTLOG:$RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn2-ITER$ITER" >> $INFO
        /home/maha/hadoop-2.2.0/bin/yarn application -appStates FINISHED -list 2>&1 | grep $DST_EVENT_LOG_FN > /dev/null 2>&1
        if [ $? -eq 0 ]
        then
            echo "TAG:spark-vcore1-cms-dyn2 ITER:$ITER STATUS:0" >> $INFO
        else
            echo "TAG:spark-vcore1-cms-dyn2 ITER:$ITER STATUS:1" >> $INFO
        fi
        # FIXME: put time result into INFO
    else
        echo "Application ID not found for TAG:spark-vcore1-cms-dyn2 ITER:$ITER"
    fi
done

CMD_TO_KILL="/home/hduser/spark-1.6.1-bin-hadoop2.3/bin/spark-shell"
echo "TAG:spark-vcore1-cms-dyn3 COUNT:1" >> $INFO
for ITER in $(seq 1)
do
    export RUN_ID="spark-vcore1-cms-dyn3-ITER$ITER"
    CMD="/home/hduser/spark-1.6.1-bin-hadoop2.3/bin/spark-shell --master yarn --executor-cores 1 --executor-memory 1g --driver-memory 4g --conf spark.executor.extraJavaOptions=\"-XX:MaxPermSize=256m -server -XX:+UseMembar -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 -XX:SurvivorRatio=4 -XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=70\" --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/sparkLogs --queue root.spark_queue.vcore1 --jars /home/maha/db-derby-10.11.1.1-bin/lib/derbyclient.jar -i /home/felix/Github/pid_monitor/workload/hive/test_case/spark-vcore1-iter4.scala"
    CMD="${CMD} > $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn3-ITER$ITER.log 2>&1"
    export WORKLOAD_CMD=${CMD}
    # Get existing application-id infos
    echo "FINISHED" > $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 FINISHED $DEBUG >> $APPID`;
    echo "FAILED" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 FAILED $DEBUG >> $APPID`;
    echo "KILLED" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 KILLED $DEBUG >> $APPID`;
    echo "RUNNING" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 RUNNING $DEBUG >> $APPID`;
    $PMH/workload/spark/scripts/query_yarn_app_id.pl $APPID $INFO spark-vcore1-cms-dyn3 $ITER /home/maha/hadoop-2.2.0 $PMH/workload/spark/scripts $DEBUG &
    ${PMH}/run-workload.sh
    DURATION=`grep "Elapsed (wall clock) time" $RUNDIR/data/raw/spark-vcore1-cms-dyn3-ITER${ITER}_time_stdout.txt | awk -F"m:ss): " '{print $2}' | awk -F: 'END { if (NF == 2) {sum=$1*60+$2} else {sum=$1*3600+$2*60+$3} print sum}'`
    echo "TAG:spark-vcore1-cms-dyn3 ITER:$ITER DURATION:$DURATION" >> $INFO
    APP_ID_FROM_LOG=""
    grep "EventLoggingListener: Logging events to" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn3-ITER$ITER.log > /dev/null 2>&1
    if [ $? -eq 0 ]
    then
        APP_ID_FROM_LOG=`grep "EventLoggingListener: Logging events to" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn3-ITER$ITER.log | awk -F"file:" '{print $2}' | awk -F/ '{print $NF}'`;
    else
        grep "Submitted application" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn3-ITER$ITER.log > /dev/null 2>&1
        if [ $? -eq 0 ]
        then
            APP_ID_FROM_LOG=`grep "Submitted application" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn3-ITER$ITER.log | awk '{print $NF}'`;
        fi
    fi

    if [ $APP_ID_FROM_LOG != "" ]
    then
        grep "TAG:spark-vcore1-cms-dyn3 ITER:$ITER APPID:$APP_ID_FROM_LOG" $INFO > /dev/null 2>&1
        if [ $? -ne 0 ]
        then
            sed -i "s/TAG:spark-vcore1-cms-dyn3 ITER:$ITER APPID:.*\$/TAG:spark-vcore1-cms-dyn3 ITER:$ITER APPID:$APP_ID_FROM_LOG/g" $INFO
        fi
    fi

    grep "TAG:spark-vcore1-cms-dyn3 ITER:$ITER APPID:" $INFO > /dev/null 2>&1
    while [ $? -ne 0 ]
    do
        sleep 1
        grep "TAG:spark-vcore1-cms-dyn3 ITER:$ITER APPID:" $INFO > /dev/null 2>&1
    done
    grep "TAG:spark-vcore1-cms-dyn3 ITER:$ITER APPID:TIMEOUT" $INFO > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        DST_EVENT_LOG_FN=`grep "TAG:spark-vcore1-cms-dyn3 ITER:$ITER APPID:" $INFO | awk -F"APPID:" '{print $2}'`;
        for SLAVE in $SLAVES
        do
            scp $SLAVE:/tmp/sparkLogs/$DST_EVENT_LOG_FN $RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn3-ITER$ITER > /dev/null 2>&1
        done
        scp namenode:/tmp/sparkLogs/$DST_EVENT_LOG_FN $RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn3-ITER$ITER > /dev/null 2>&1
        echo "TAG:spark-vcore1-cms-dyn3 ITER:$ITER EVENTLOG:$RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn3-ITER$ITER" >> $INFO
        /home/maha/hadoop-2.2.0/bin/yarn application -appStates FINISHED -list 2>&1 | grep $DST_EVENT_LOG_FN > /dev/null 2>&1
        if [ $? -eq 0 ]
        then
            echo "TAG:spark-vcore1-cms-dyn3 ITER:$ITER STATUS:0" >> $INFO
        else
            echo "TAG:spark-vcore1-cms-dyn3 ITER:$ITER STATUS:1" >> $INFO
        fi
        # FIXME: put time result into INFO
    else
        echo "Application ID not found for TAG:spark-vcore1-cms-dyn3 ITER:$ITER"
    fi
done

CMD_TO_KILL="/home/hduser/spark-1.6.1-bin-hadoop2.3/bin/spark-shell"
echo "TAG:spark-vcore1-cms-dyn4 COUNT:1" >> $INFO
for ITER in $(seq 1)
do
    export RUN_ID="spark-vcore1-cms-dyn4-ITER$ITER"
    CMD="/home/hduser/spark-1.6.1-bin-hadoop2.3/bin/spark-shell --master yarn --executor-cores 1 --executor-memory 1g --driver-memory 4g --conf spark.executor.extraJavaOptions=\"-XX:MaxPermSize=256m -server -XX:+UseMembar -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 -XX:SurvivorRatio=4 -XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=70\" --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/sparkLogs --queue root.spark_queue.vcore1 --jars /home/maha/db-derby-10.11.1.1-bin/lib/derbyclient.jar -i /home/felix/Github/pid_monitor/workload/hive/test_case/spark-vcore1-iter8.scala"
    CMD="${CMD} > $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn4-ITER$ITER.log 2>&1"
    export WORKLOAD_CMD=${CMD}
    # Get existing application-id infos
    echo "FINISHED" > $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 FINISHED $DEBUG >> $APPID`;
    echo "FAILED" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 FAILED $DEBUG >> $APPID`;
    echo "KILLED" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 KILLED $DEBUG >> $APPID`;
    echo "RUNNING" >> $APPID
    `$PMH/workload/spark/scripts/query_yarn_app_id_in_some_state.pl /home/maha/hadoop-2.2.0 RUNNING $DEBUG >> $APPID`;
    $PMH/workload/spark/scripts/query_yarn_app_id.pl $APPID $INFO spark-vcore1-cms-dyn4 $ITER /home/maha/hadoop-2.2.0 $PMH/workload/spark/scripts $DEBUG &
    ${PMH}/run-workload.sh
    DURATION=`grep "Elapsed (wall clock) time" $RUNDIR/data/raw/spark-vcore1-cms-dyn4-ITER${ITER}_time_stdout.txt | awk -F"m:ss): " '{print $2}' | awk -F: 'END { if (NF == 2) {sum=$1*60+$2} else {sum=$1*3600+$2*60+$3} print sum}'`
    echo "TAG:spark-vcore1-cms-dyn4 ITER:$ITER DURATION:$DURATION" >> $INFO
    APP_ID_FROM_LOG=""
    grep "EventLoggingListener: Logging events to" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn4-ITER$ITER.log > /dev/null 2>&1
    if [ $? -eq 0 ]
    then
        APP_ID_FROM_LOG=`grep "EventLoggingListener: Logging events to" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn4-ITER$ITER.log | awk -F"file:" '{print $2}' | awk -F/ '{print $NF}'`;
    else
        grep "Submitted application" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn4-ITER$ITER.log > /dev/null 2>&1
        if [ $? -eq 0 ]
        then
            APP_ID_FROM_LOG=`grep "Submitted application" $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic-20170117014953/spark-vcore1-cms-dyn4-ITER$ITER.log | awk '{print $NF}'`;
        fi
    fi

    if [ $APP_ID_FROM_LOG != "" ]
    then
        grep "TAG:spark-vcore1-cms-dyn4 ITER:$ITER APPID:$APP_ID_FROM_LOG" $INFO > /dev/null 2>&1
        if [ $? -ne 0 ]
        then
            sed -i "s/TAG:spark-vcore1-cms-dyn4 ITER:$ITER APPID:.*\$/TAG:spark-vcore1-cms-dyn4 ITER:$ITER APPID:$APP_ID_FROM_LOG/g" $INFO
        fi
    fi

    grep "TAG:spark-vcore1-cms-dyn4 ITER:$ITER APPID:" $INFO > /dev/null 2>&1
    while [ $? -ne 0 ]
    do
        sleep 1
        grep "TAG:spark-vcore1-cms-dyn4 ITER:$ITER APPID:" $INFO > /dev/null 2>&1
    done
    grep "TAG:spark-vcore1-cms-dyn4 ITER:$ITER APPID:TIMEOUT" $INFO > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        DST_EVENT_LOG_FN=`grep "TAG:spark-vcore1-cms-dyn4 ITER:$ITER APPID:" $INFO | awk -F"APPID:" '{print $2}'`;
        for SLAVE in $SLAVES
        do
            scp $SLAVE:/tmp/sparkLogs/$DST_EVENT_LOG_FN $RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn4-ITER$ITER > /dev/null 2>&1
        done
        scp namenode:/tmp/sparkLogs/$DST_EVENT_LOG_FN $RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn4-ITER$ITER > /dev/null 2>&1
        echo "TAG:spark-vcore1-cms-dyn4 ITER:$ITER EVENTLOG:$RUNDIR/spark_events/${DST_EVENT_LOG_FN}-spark-vcore1-cms-dyn4-ITER$ITER" >> $INFO
        /home/maha/hadoop-2.2.0/bin/yarn application -appStates FINISHED -list 2>&1 | grep $DST_EVENT_LOG_FN > /dev/null 2>&1
        if [ $? -eq 0 ]
        then
            echo "TAG:spark-vcore1-cms-dyn4 ITER:$ITER STATUS:0" >> $INFO
        else
            echo "TAG:spark-vcore1-cms-dyn4 ITER:$ITER STATUS:1" >> $INFO
        fi
        # FIXME: put time result into INFO
    else
        echo "Application ID not found for TAG:spark-vcore1-cms-dyn4 ITER:$ITER"
    fi
done

$PMH/workload/spark/scripts/create_summary_table.pl $PMH/workload/spark/test_case/spark_sql_vcore1_cms_dynamic.json $RUNDIR

