#!/bin/bash

FOLDER=$1
exec_with_time_log () {
    begin_time=`date +%s`
    $1 > $2 2>&1
    end_time=`date +%s`
    echo "$((end_time - begin_time))" >> ${FOLDER}/hive_simple_duration.log
}

exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where2_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_where1_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere1_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere2_simple2.sql" "/dev/null"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f tpcds_hive_nowhere3_simple2.sql" "/dev/null"
