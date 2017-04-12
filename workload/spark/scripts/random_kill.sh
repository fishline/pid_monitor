#!/bin/bash

while [ 1 ]
do
    sleep `shuf -i 30-50 -n 1`
    TMP=`shuf -i 1-10 -n 1`
    if [ $TMP -gt 6 ]
    then
        date
        echo "kill task on Power node"
        ID=`shuf -i 1-10 -n 1`
        SIGNAL=`shuf -e 9 15 -n 1`
        ssh -l test${ID} datanode5 "ps -ef | grep java_real | grep test${ID} | grep -v grep | shuf | head -n 1 | awk '{print \$2}' | xargs -i kill -${SIGNAL} {}"
    else
        date
        echo "kill task on Master node"
        ID=`shuf -i 1-10 -n 1`
        ssh -l test${ID} master "yarn application -list 2>&1 | grep application_ | shuf | head -n 1 | awk '{print \$1}' | xargs -i yarn application -kill {}"
    fi
done
