#!/bin/bash

if [ $# -lt 2 ]
then
    echo "Usage: ./os_clean_cache.sh <TGT_HOST> <SPARK_HOME>"
    exit 1
fi

TGT_HOST=$1
SPARK_HOME=$2

ssh $TGT_HOST "sync && echo 3 > /proc/sys/vm/drop_caches"
ssh -T $TGT_HOST << EOF
cd $SPARK_HOME 
grep -v \# conf/slaves | xargs -i ssh {} "sync && echo 3 > /proc/sys/vm/drop_caches"
EOF
