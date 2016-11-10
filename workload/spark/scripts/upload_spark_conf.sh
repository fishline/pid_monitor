#!/bin/bash

if [ -z ${PMH} ]
then
    echo "Please set pid_monitor_home in var PMH"
    exit 1
fi

if [ $# -lt 3 ]
then
    echo "Usage: ./upload_spark_conf.sh <TGT_HOST> <SPARK_HOME> <SPARK_CONF_SUFFIX>"
    exit 1
fi

TGT_HOST=$1
SPARK_HOME=$2
SUFFIX=$3

scp ${PMH}/workload/spark/conf/spark-defaults.conf.$SUFFIX $TGT_HOST:$SPARK_HOME/conf/spark-defaults.conf
scp ${PMH}/workload/spark/conf/spark-env.sh.$SUFFIX $TGT_HOST:$SPARK_HOME/conf/spark-env.sh
ssh -T $TGT_HOST << EOF
cd $SPARK_HOME/conf
grep -v \# slaves | xargs -i scp spark-defaults.conf {}:\`pwd\`
EOF
ssh -T $TGT_HOST << EOF
cd $SPARK_HOME/conf
grep -v \# slaves | xargs -i scp spark-env.sh {}:\`pwd\`
EOF
