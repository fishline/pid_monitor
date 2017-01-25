#!/bin/bash

for i in `seq 8`
do
    ./spark_user.sh &
    sleep 10
done

wait
