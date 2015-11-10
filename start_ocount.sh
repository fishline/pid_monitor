#!/bin/bash

[ "$#" -lt "5" ] && echo Usage: $0 HOSTNAME OCOUNT_FN DELAY_SEC PID EVENT_LIST && exit 1

ARGS=( $@ )
len=${#ARGS[@]}
HOST=${ARGS[0]}
OCOUNT_FN=/tmp/pid_monitor/$(basename ${ARGS[1]})
DELAY_SEC=${ARGS[2]}
PID=${ARGS[3]}
EVENT_LIST=${ARGS[@]:4:$len-1}

[ "$DELAY_SEC" -lt "1" ] && echo Setting DELAY_SEC to 1 instead of $3; DELAY_SEC=1

echo Ocount Monitoring PID=$PID on $HOST
#echo Events are $EVENT_LIST

OCOUNT_CMD="mkdir -p /tmp/pid_monitor/; \
           chmod 777 /tmp/pid_monitor; \
           rm -f $OCOUNT_FN; \
           sleep 0.1; \
           ocount -i $((DELAY_SEC*1000)) --events ${EVENT_LIST} -p $PID  >> $OCOUNT_FN 2>&1 < /dev/null &"
echo $OCOUNT_CMD

$(ssh $HOST $OCOUNT_CMD) 2>/dev/null &
