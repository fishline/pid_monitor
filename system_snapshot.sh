#!/bin/bash

[ "$#" -ne "1" ] && echo Usage: $0 HOSTNAME && exit 1
HOST=$1
if [ "$HOST" == "$(hostname -s)" ] || [ "$HOST" == "$(hostname -s)" ]
then
  unset SSH_FLAG
else
  SSH_FLAG=1
fi

TMPDIR=/tmp/${USER}_PM

echo Collecting system snapshot on $HOST

CMD="mkdir -p $TMPDIR; \
    cd $TMPDIR; \
    [ ! -e linux_summary ] && git clone https://github.com/jschaub30/linux_summary && FLAG=1; \
    cd linux_summary; \
    [ -z \${FLAG+x} ] && git pull; \
    ./linux_summary.sh;"

if [ $SSH_FLAG ]
then
    $(ssh $HOST $CMD) 2>/dev/null 
    scp $HOST:$TMPDIR/linux_summary/index.html $HOST.html
else
    bash -c "$CMD" 2>&1 > /dev/null
    cp $TMPDIR/linux_summary/index.html $HOST.html
fi

