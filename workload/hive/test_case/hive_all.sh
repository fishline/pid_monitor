#!/bin/bash

for i in `seq 30`
do
    ./hive_user.sh &
    sleep 5
done

wait
