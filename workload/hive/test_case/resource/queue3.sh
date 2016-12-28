#!/bin/bash

hive --database tpcds_bin_partitioned_orc_1000 -f ${PMH}/workload/hive/test_case/resource/query19.sql > /dev/null 2>&1
hive --database tpcds_bin_partitioned_orc_1000 -f ${PMH}/workload/hive/test_case/resource/query97.sql > /dev/null 2>&1
