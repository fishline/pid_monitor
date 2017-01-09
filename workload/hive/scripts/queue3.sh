#!/bin/bash

FOLDER=$1
hive --database tpcds_bin_partitioned_orc_1000 -f query19.sql > ./${FOLDER}/query19.sql.log 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f query97.sql > ./${FOLDER}/query97.sql.log 2>&1

#hive --database tpcds_bin_partitioned_orc_1000 -f query24.sql > ./${FOLDER}/query24.sql.log 2>&1
#hive --database tpcds_bin_partitioned_orc_1000 -f query64.sql > ./${FOLDER}/query64.sql.log 2>&1
#hive --database tpcds_bin_partitioned_orc_1000 -f query72.sql > ./${FOLDER}/query72.sql.log 2>&1
