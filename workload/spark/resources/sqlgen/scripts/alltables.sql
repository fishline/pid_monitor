create database if not exists ${DB};
use ${DB};

drop table if exists os_order;
create external table os_order(
      oid         int               
,     bid         bigint              
,     day         date                         
,     t           string                         
,     dummy       string                       
)
row format delimited fields terminated by '\t' 
location '${LOCATION}/OS_ORDER.txt';

drop table if exists os_order_item;
create external table os_order_item(
      iid         int               
,     oid         int              
,     gid         int                         
,     gnum        float                        
,     gprice      float                       
,     gsell       float                       
,     dummy       string                       
)
row format delimited fields terminated by '\t' 
location '${LOCATION}/OS_ORDER_ITEM.txt';
