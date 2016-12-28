#!/bin/bash

hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where1_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where2_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where3_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where1_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where3_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where2_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where3_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where2_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1_queue1.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_queue1.sql > /dev/null 2>&1
