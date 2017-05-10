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
        TMP2=`shuf -i 1-10 -n 1`
        if [ $TMP2 -lt 7 ]
        then
            ssh -l tdwadmin datanode5 "ps -ef | grep java_real | grep tdwadmin | grep -v grep | shuf | head -n 1 | awk '{print \$2}' | xargs -i kill -${SIGNAL} {}"
        else 
            ssh -l test${ID} datanode5 "ps -ef | grep java_real | grep test${ID} | grep -v grep | shuf | head -n 1 | awk '{print \$2}' | xargs -i kill -${SIGNAL} {}"
        fi
    else
        date
        echo "kill task on Master node"
        ID=`shuf -i 1-10 -n 1`
        TMP2=`shuf -i 1-10 -n 1`
        if [ $TMP2 -lt 7 ]
        then
            ssh -l tdwadmin master "yarn application -list 2>&1 | grep application_ | grep tdwadmin | shuf | head -n 1 | awk '{print \$1}' | xargs -i yarn application -kill {}"
        else
            ssh -l test${ID} master "yarn application -list 2>&1 | grep application_ | grep test${ID} | shuf | head -n 1 | awk '{print \$1}' | xargs -i yarn application -kill {}"
        fi
    fi
done
