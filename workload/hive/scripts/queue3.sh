#!/bin/bash

FOLDER=$1
hive --database tpcds_bin_partitioned_orc_1000 -f query19.sql > ./${FOLDER}/query19.sql.log 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f query97.sql > ./${FOLDER}/query97.sql.log 2>&1

