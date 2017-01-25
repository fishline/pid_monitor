#!/bin/bash

for i in `seq 20`
do
    SEQ=`shuf -i 1-4 -n 1`
    /home/test/spark-1.6.1-bin-hadoop2.3/bin/spark-shell --master yarn --num-executors 1 --executor-cores 1 --executor-memory 2g --driver-memory 4g --conf spark.executor.extraJavaOptions="-XX:MaxPermSize=256m -server -XX:+UseMembar -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 -XX:SurvivorRatio=4 -XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+UnlockExperimentalVMOptions -XX:+UseCriticalCompilerThreadPriority" --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/sparkLogs --jars /home/maha/db-derby-10.11.1.1-bin/lib/derbyclient.jar -i /home/test/pid_monitor/workload/hive/test_case/scala/spark_q${SEQ}.scala
done
