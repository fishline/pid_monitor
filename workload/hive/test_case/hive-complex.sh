#!/bin/bash

FOLDER=$1
exec_with_time_log () {
    begin_time=`date +%s`
    $1 > $2 2>&1
    end_time=`date +%s`
    echo "$((end_time - begin_time))" >> ${FOLDER}/hive_complex_duration.log
}

exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f query68_complex_iter4.sql" "./${FOLDER}/query68_complex_iter4.sql.log"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f query97_complex.sql" "./${FOLDER}/query97_complex.sql.log"
exec_with_time_log "hive --database tpcds_bin_partitioned_orc_1000 -f query19_complex_iter8.sql" "./${FOLDER}/query19_complex_iter8.sql.log"

