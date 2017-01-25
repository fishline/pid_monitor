#!/bin/bash

if [ $# -lt 2 ]
then
    echo "Usage: ./convert_to_hive_orc_lzo_table.sh <HDFS source folder> <hive DB name>";
    exit 1
fi

HDFS_LOC=$1
HIVE_DB=$2
TEXT_DB=${HIVE_DB}_text

hive -i load-flat.sql -f alltables.sql -d DB=${TEXT_DB} -d LOCATION=${HDFS_LOC}
hive -i load-partitioned.sql -f os_order.sql -d DB=${HIVE_DB} -d SOURCE=${TEXT_DB}
hive -i load-partitioned.sql -f os_order_item.sql -d DB=${HIVE_DB} -d SOURCE=${TEXT_DB}

