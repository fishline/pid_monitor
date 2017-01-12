#!/bin/bash

CORE_COUNT=$1
CONTAINER_ID=$2

(
flock -x 200
CORE_LIST=`/root/scripts/do_allocate_vcore.pl ${CORE_COUNT} ${CONTAINER_ID}`
echo ${CORE_LIST}
) 200>/var/lock/yarnlockfile
