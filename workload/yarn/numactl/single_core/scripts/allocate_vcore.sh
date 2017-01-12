#!/bin/bash

CONTAINER_ID=$1

(
flock -x 200
CORE=`/root/scripts/do_allocate_vcore.pl`
echo "VCPU:${CORE} CONTAINER:${CONTAINER_ID}" >> /tmp/vcore.log
echo ${CORE}
) 200>/var/lock/yarnlockfile
