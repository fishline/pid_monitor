#!/bin/bash

function levenshtein {
	if [ "$#" -ne "2" ]; then
		echo "Usage: $0 word1 word2" >&2
	elif [ "${#1}" -lt "${#2}" ]; then
		levenshtein "$2" "$1"
	else
		local str1len=$((${#1}))
		local str2len=$((${#2}))
		local d i j
		for i in $(seq 0 $(((str1len+1)*(str2len+1)))); do
			d[i]=0
		done
		for i in $(seq 0 $((str1len)));	do
			d[$((i+0*str1len))]=$i
		done
		for j in $(seq 0 $((str2len)));	do
			d[$((0+j*(str1len+1)))]=$j
		done

		for j in $(seq 1 $((str2len))); do
			for i in $(seq 1 $((str1len))); do
				[ "${1:i-1:1}" = "${2:j-1:1}" ] && local cost=0 || local cost=1
				local del=$((d[(i-1)+str1len*j]+1))
				local ins=$((d[i+str1len*(j-1)]+1))
				local alt=$((d[(i-1)+str1len*(j-1)]+cost))
				d[i+str1len*j]=$(echo -e "$del\n$ins\n$alt" | sort -n | head -1)
			done
		done
		echo ${d[str1len+str1len*(str2len)]}
	fi
}

if [ "$#" -eq 1 ]
then
    FINISH_SEC=`date +%s`
    FINISH_SEC=`expr $FINISH_SEC \- 120`
    FINISH_SEC=`expr $FINISH_SEC \/ 60`
    FINISH_SEC=`expr $FINISH_SEC \* 60`
    DELTA_SEC=`expr $1 \* 3600`
    BEGIN_SEC=`expr $FINISH_SEC \- $DELTA_SEC`
    END_T=`date -d@$FINISH_SEC +%Y"/"%m"/"%d" "%T`
    BEGIN_T=`date -d@$BEGIN_SEC +%Y"/"%m"/"%d" "%T`

    FIRST_HALF=`echo $BEGIN_T | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
    SECOND_HALF=`echo $END_T | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
    mkdir ./${FIRST_HALF}--${SECOND_HALF}

    BEGIN_TIME=$BEGIN_SEC
    END_TIME=$FINISH_SEC
    COUNT=1
elif [ "$#" -eq 2 ]
then
    #FIRST_HALF=`echo $1 | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
    #SECOND_HALF=`echo $2 | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
    #mkdir ./${FIRST_HALF}--${SECOND_HALF}
    #
    #BEGIN_TIME=`date --date="$1" +"%s"`
    #END_TIME=`date --date="$2" +"%s"`
    #BEGIN_T=$1
    #END_T=$2

    FINISH_SEC=`date +%s`
    FINISH_SEC=`expr $FINISH_SEC \- 120`
    FINISH_SEC=`expr $FINISH_SEC \/ 60`
    FINISH_SEC=`expr $FINISH_SEC \* 60`
    DELTA_SEC=`expr $1 \* 3600`
    COUNT=$2
    MULTI_DELTA=`expr $DELTA_SEC \* $COUNT`
    BEGIN_SEC=`expr $FINISH_SEC \- $MULTI_DELTA`
    FINISH_SEC=`expr $BEGIN_SEC \+ $DELTA_SEC`
    END_T=`date -d@$FINISH_SEC +%Y"/"%m"/"%d" "%T`
    BEGIN_T=`date -d@$BEGIN_SEC +%Y"/"%m"/"%d" "%T`

    FIRST_HALF=`echo $BEGIN_T | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
    SECOND_HALF=`echo $END_T | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
    mkdir ./${FIRST_HALF}--${SECOND_HALF}

    BEGIN_TIME=$BEGIN_SEC
    END_TIME=$FINISH_SEC
elif [ "$#" -ne 2 ]
then
    echo "Usage: ./collect_hive_spark_data.sh <collect duration in hour> <collect how many times>"
    exit 1
fi

while [ $COUNT -ge 1 ]
do
    # Handle MR history
    cd ./${FIRST_HALF}--${SECOND_HALF}
    FOLDER=./
    rm -f app
    MASTER=`ip route show | grep ^default | awk '{print $5}' | xargs -i ifconfig {} | grep netmask | awk '{print $2}'`
    wget http://${MASTER}:19888/jobhistory/app > /dev/null 2>&1
    echo "BEGIN_T: $BEGIN_T"
    echo "END_T: $END_T"
    ../../../../hive/scripts/filter_history_290.pl "$BEGIN_T" "$END_T" app | xargs -n 1 -P 8 -i ../../../../hive/scripts/wget_mapreduce_job_history.pl ${MASTER} 19888 {} $FOLDER
    rm -f app

    # Handle Spark history
    #PAGE=1
    #while [ $PAGE -lt 1000 ]
    #do
    #    rm -f index.html
    #    wget http://${MASTER}:18080/?page=${PAGE}\&showIncomplete=false -O index.html > /dev/null 2>&1
    #    ../../../../spark/scripts/filter_history.pl "$BEGIN_T" "$END_T" index.html | xargs -n 1 -P 8 -i \cp /tmp/sparkLogs/{} $FOLDER/
    #    PAGE=`expr $PAGE \+ 1`
    #    ts=`grep "sorttable_customkey" index.html | sed -n 2p | awk -F\" '{print \$2}'`
    #    ts=`expr $ts \/ 1000`
    #    if [ $ts -lt $BEGIN_TIME ]
    #    then
    #        break
    #    fi
    #done
    #rm -f index.html
    ls /tmp/sparkLogs/ | grep -v inprogress | xargs -i sh -c "echo \";\"{}; cat /tmp/sparkLogs/{} 2>&1 | grep -E -e \"SparkListenerApplicationStart|SparkListenerApplicationEnd\" | tr ',' '\n' | grep Timestamp | tr -d '}' | awk -F: 'BEGIN {cnt=0} {cnt++; print \$2} END {if(cnt==0){print \"open fail\"}}'" | tr '\n' ' ' | tr ';' '\n' | grep -v fail | grep -v "^\$" | awk "{start=\$2/1000; stop=\$3/1000; if (start>$BEGIN_SEC && stop<$FINISH_SEC) {print \$1}}" | xargs -i sh -c "\cp /tmp/sparkLogs/{} $FOLDER/"

    DELTA=`expr $END_TIME \- $BEGIN_TIME`
    echo "Test duration: $DELTA sec" > ${FOLDER}/result.log

    # Dump hive data
    #rm -f ${FOLDER}/stats.log
    ls ${FOLDER} | grep ^job_ | xargs -i ../../../../hive/scripts/mapreduce_statistics_hadoop290.pl ${FOLDER}/{} >> ${FOLDER}/stats.log

    echo "hive MAP_COUNT:" >> ${FOLDER}/result.log
    GOT_DATA=0
    for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
    do
        GOT_DATA=1
        cat ${FOLDER}/stats.log  | grep $host | grep MAP_COUNT | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    done
    if [ $GOT_DATA -eq 0 ]
    then
        echo "NA 0" >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    fi

    echo "hive REDUCE_COUNT:" >> ${FOLDER}/result.log
    GOT_DATA=0
    for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
    do
        GOT_DATA=1
        cat ${FOLDER}/stats.log  | grep $host | grep RED_COUNT | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    done
    if [ $GOT_DATA -eq 0 ]
    then
        echo "NA 0" >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    fi

    echo "hive MAP_HIVE_RECORDS_IN:" >> ${FOLDER}/result.log
    GOT_DATA=0
    for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
    do
        GOT_DATA=1
        cat ${FOLDER}/stats.log  | grep $host | grep MAP_HIVE_RECORDS_IN | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    done
    if [ $GOT_DATA -eq 0 ]
    then
        echo "NA 0" >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    fi

    echo "hive MAP_HDFS_BYTES_READ:" >> ${FOLDER}/result.log
    GOT_DATA=0
    for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
    do
        GOT_DATA=1
        cat ${FOLDER}/stats.log  | grep $host | grep MAP_HDFS_BYTES_READ | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    done
    if [ $GOT_DATA -eq 0 ]
    then
        echo "NA 0" >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    fi

    echo "hive TOTAL_MAP_DURATION(ms):" >> ${FOLDER}/result.log
    GOT_DATA=0
    for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
    do
        GOT_DATA=1
        cat ${FOLDER}/stats.log  | grep $host | grep TOTAL_MAP_DURATION | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    done
    if [ $GOT_DATA -eq 0 ]
    then
        echo "NA 0" >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    fi

    echo "hive TOTAL_MAP_CPU_TIME(ms):" >> ${FOLDER}/result.log
    GOT_DATA=0
    for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
    do
        GOT_DATA=1
        cat ${FOLDER}/stats.log  | grep $host | grep TOTAL_MAP_CPU_TIME | awk '{print $2}' | awk -v var="$host" -F: '{sum+=$2} END {print var " " sum}' >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    done
    if [ $GOT_DATA -eq 0 ]
    then
        echo "NA 0" >> ${FOLDER}/result.log
        echo "" >> ${FOLDER}/result.log
    fi

    # Categorize spark event log by "App Name"
    APP_NAME_GROUPS=( $(grep -nr "\"App Name\"" ${FOLDER}/application_* 2>/dev/null | awk -F"\"App Name\":" '{print $2}' | awk -F"\",\"" '{print $1}' | tr -d '^"' | sort -n | uniq) )
    NUM_APP_NAMES=${#APP_NAME_GROUPS[@]}
    for I in `seq 1 $NUM_APP_NAMES`
    do
        I=`expr $I \- 1`
        mkdir ${FOLDER}/SPARK_APP_NAME_${I}
    done

    for LOG in `ls ${FOLDER}/application_*`
    do
        LOG_APP_NAME=`grep -nr "\"App Name\"" ${LOG} 2>/dev/null | awk -F"\"App Name\":" '{print $2}' | awk -F"\",\"" '{print $1}' | tr -d '^"' | head -n 1 | tr -d '\n'`
        MIN_DIST=10000
        MIN_DIST_I=10000
        for I in `seq 1 $NUM_APP_NAMES`
        do
            I=`expr $I \- 1`
            DISTANCE=$(levenshtein "${LOG_APP_NAME}" "${APP_NAME_GROUPS[I]}")
            if [ $DISTANCE -lt $MIN_DIST ]
            then
                MIN_DIST=$DISTANCE
                MIN_DIST_I=$I
            fi
        done
        mv ${LOG} ${FOLDER}/SPARK_APP_NAME_${MIN_DIST_I}/
    done

    for I in `seq 1 $NUM_APP_NAMES`
    do
        I=`expr $I \- 1`
        echo "spark APP_NAME_GROUP_${I} task count:" >> ${FOLDER}/result.log
        GOT_DATA=0
        for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
        do
            GOT_DATA=1
            COUNT_SPARK=`grep "SparkListenerTaskEnd" ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep $host | grep Success | wc -l`
            echo "$host $COUNT_SPARK" >> ${FOLDER}/result.log
            echo "" >> ${FOLDER}/result.log
        done
        if [ $GOT_DATA -eq 0 ]
        then
            for host in `cat ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep "\"Host\"" | grep -v "Block Manager ID" | awk -F"\"Host\":" '{print $2}' | awk -F, '{print $1}' | sort -n | uniq | tr -d '"'`
            do
                GOT_DATA=1
                COUNT_SPARK=`grep "SparkListenerTaskEnd" ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep $host | grep Success | wc -l`
                echo "$host $COUNT_SPARK" >> ${FOLDER}/result.log
                echo "" >> ${FOLDER}/result.log
            done
            if [ $GOT_DATA -eq 0 ]
            then
                echo "NA 0" >> ${FOLDER}/result.log
                echo "" >> ${FOLDER}/result.log
            fi
        fi
    
        echo "spark APP_NAME_GROUP_${I} bytes read:" >> ${FOLDER}/result.log
        GOT_DATA=0
        for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
        do
            GOT_DATA=1
            grep "SparkListenerTaskEnd" ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep $host | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -v var="$host" -F: 'BEGIN {sum=0;} {sum+=$2} END {print var " "sum}' >> ${FOLDER}/result.log
            echo "" >> ${FOLDER}/result.log
        done
        if [ $GOT_DATA -eq 0 ]
        then
            for host in `cat ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep "\"Host\"" | grep -v "Block Manager ID" | awk -F"\"Host\":" '{print $2}' | awk -F, '{print $1}' | sort -n | uniq | tr -d '"'`
            do
                GOT_DATA=1
                grep "SparkListenerTaskEnd" ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep $host | grep Success | grep "Bytes Read" | awk -F"Bytes Read" '{print $2}' | awk -F, '{print $1}' | awk -v var="$host" -F: 'BEGIN {sum=0;} {sum+=$2} END {print var " "sum}' >> ${FOLDER}/result.log
                echo "" >> ${FOLDER}/result.log
            done
            if [ $GOT_DATA -eq 0 ]
            then
                echo "NA 0" >> ${FOLDER}/result.log
                echo "" >> ${FOLDER}/result.log
            fi
        fi
    
        echo "spark APP_NAME_GROUP_${I} records read:" >> ${FOLDER}/result.log
        GOT_DATA=0
        for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
        do
            GOT_DATA=1
            grep "SparkListenerTaskEnd" ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep $host | grep Success | grep "Records Read" | awk -F"Records Read" '{print $2}' | awk -F\} '{print $1}' | awk -v var="$host" -F: 'BEGIN {sum=0;} {sum+=$2} END {print var " "sum}' >> ${FOLDER}/result.log
            echo "" >> ${FOLDER}/result.log
        done
        if [ $GOT_DATA -eq 0 ]
        then
            for host in `cat ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep "\"Host\"" | grep -v "Block Manager ID" | awk -F"\"Host\":" '{print $2}' | awk -F, '{print $1}' | sort -n | uniq | tr -d '"'`
            do
                GOT_DATA=1
                grep "SparkListenerTaskEnd" ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep $host | grep Success | grep "Records Read" | awk -F"Records Read" '{print $2}' | awk -F\} '{print $1}' | awk -v var="$host" -F: 'BEGIN {sum=0;} {sum+=$2} END {print var " "sum}' >> ${FOLDER}/result.log
                echo "" >> ${FOLDER}/result.log
            done
            if [ $GOT_DATA -eq 0 ]
            then
                echo "NA 0" >> ${FOLDER}/result.log
                echo "" >> ${FOLDER}/result.log
            fi
        fi
    
        echo "spark APP_NAME_GROUP_${I} task avg launch-finish time(ms):" >> ${FOLDER}/result.log
        GOT_DATA=0
        for host in `cat ${FOLDER}/stats.log | awk '{print \$1}' | sort -n | uniq | awk -F: '{print \$2}'`
        do
            GOT_DATA=1
            grep "SparkListenerTaskEnd" ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep $host | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Launch Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file1
            grep "SparkListenerTaskEnd" ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep $host | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Finish Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file2
            paste file1 file2 | column -s $' ' -t | awk -v var="$host" 'BEGIN{sum=0; count=0;} {delta=($2 - $1); sum+=delta; count+=1} END {if (count==0){ print var " 0"} else {print var " " sum/count}}' >> ${FOLDER}/result.log
            echo "" >> ${FOLDER}/result.log
            rm -f file1
            rm -f file2
        done
        if [ $GOT_DATA -eq 0 ]
        then
            for host in `cat ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep "\"Host\"" | grep -v "Block Manager ID" | awk -F"\"Host\":" '{print $2}' | awk -F, '{print $1}' | sort -n | uniq | tr -d '"'`
            do
                GOT_DATA=1
                grep "SparkListenerTaskEnd" ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep $host | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Launch Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file1
                grep "SparkListenerTaskEnd" ${FOLDER}/SPARK_APP_NAME_${I}/application_* | grep $host | grep Success | grep "Launch Time" | grep "Finish Time" | awk -F"Finish Time" '{print $2}' | awk -F, '{print $1}' | awk -F: '{print $2}' > file2
                paste file1 file2 | column -s $' ' -t | awk -v var="$host" 'BEGIN{sum=0; count=0;} {delta=($2 - $1); sum+=delta; count+=1} END {if (count==0){ print var " 0"} else {print var " " sum/count}}' >> ${FOLDER}/result.log
                echo "" >> ${FOLDER}/result.log
                rm -f file1
                rm -f file2
            done
            if [ $GOT_DATA -eq 0 ]
            then
                echo "NA NA" >> ${FOLDER}/result.log
                echo "" >> ${FOLDER}/result.log
            fi
        fi
    done

    rm -rf ${FOLDER}/job_*
    rm -rf ${FOLDER}/application_*
    rm -rf ${FOLDER}/SPARK_APP_NAME_*
    `cat ${FOLDER}/result.log | grep -A 1 "hive MAP_COUNT" | tail -n 1 | awk '{print $2}' | xargs -i sh -c "echo hive map count: {} > ${FOLDER}/summary.log"`
    HIVE_TOTAL_MS=`cat ${FOLDER}/result.log | grep -A 1 "hive TOTAL_MAP_DURATION" | tail -n 1 | awk '{print $2}' | tr -d '\n'`
    HIVE_TOTAL_MAP_CNT=`cat ${FOLDER}/result.log | grep -A 1 "hive MAP_COUNT" | tail -n 1 | awk '{print $2}' | tr -d '\n'`
    if [ $HIVE_TOTAL_MAP_CNT -eq 0 ]
    then
        `echo hive map avg ms: NA >> ${FOLDER}/summary.log`
    else
        HIVE_MAP_AVG_MS=`expr $HIVE_TOTAL_MS \/ $HIVE_TOTAL_MAP_CNT | tr -d '\n'`;
        `echo hive map avg ms: $HIVE_MAP_AVG_MS >> ${FOLDER}/summary.log`
    fi

    for I in `seq 1 $NUM_APP_NAMES`
    do
        I=`expr $I \- 1`
        `cat ${FOLDER}/result.log | grep -A 1 "spark APP_NAME_GROUP_${I} task count" | tail -n 1 | awk '{print $2}' | xargs -i sh -c "echo ${APP_NAME_GROUPS[I]} spark task count: {} >> ${FOLDER}/summary.log"`
        `cat ${FOLDER}/result.log | grep -A 1 "spark APP_NAME_GROUP_${I} task avg launch-finish time" | tail -n 1 | awk '{print $2}' | xargs -i sh -c "echo ${APP_NAME_GROUPS[I]} spark task avg ms: {} >> ${FOLDER}/summary.log"`
    done

    NMONFile=`ls -tr ../rundir/*/latest/nmon/*.nmon|tail -n 1`
    \cp ${NMONFile} ${FOLDER}/
    cd ../
    COUNT=`expr $COUNT \- 1`

    if [ $COUNT -ge 1 ]
    then
        BEGIN_SEC=`expr $BEGIN_SEC \+ $DELTA_SEC`
        FINISH_SEC=`expr $FINISH_SEC \+ $DELTA_SEC`
        END_T=`date -d@$FINISH_SEC +%Y"/"%m"/"%d" "%T`
        BEGIN_T=`date -d@$BEGIN_SEC +%Y"/"%m"/"%d" "%T`

        FIRST_HALF=`echo $BEGIN_T | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
        SECOND_HALF=`echo $END_T | tr '\/' '_' | tr ':' '-' | tr ' ' '-'`
        mkdir ./${FIRST_HALF}--${SECOND_HALF}

        BEGIN_TIME=$BEGIN_SEC
        END_TIME=$FINISH_SEC
    fi
done

exit 0
