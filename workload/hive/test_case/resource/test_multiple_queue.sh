#!/bin/bash

FOLDER=$1
./queue1.sh $FOLDER &
./queue2.sh $FOLDER &
./queue3.sh $FOLDER &
wait
