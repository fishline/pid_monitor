#!/bin/bash

CONTAINER_ID=$1

(
flock -x 200
`/root/scripts/do_release_vcore.pl ${CONTAINER_ID}`
) 200>/var/lock/yarnlockfile

