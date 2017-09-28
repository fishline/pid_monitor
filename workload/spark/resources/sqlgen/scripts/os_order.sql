create database if not exists ${DB};
use ${DB};

drop table if exists os_order;

create table os_order
(
      oid         int               
,     bid         bigint              
,     day         date                         
,     t           string                         
,     dummy       string                       
)
stored as rcfile;
-- stored as orc tblproperties ("orc.compress"="NONE");
-- orc does not work with lzo, store as eith "orc" or "textfile"

insert overwrite table os_order 
select
        oo.oid,
        oo.bid,
        oo.day,
        oo.t,
        oo.dummy
      from ${SOURCE}.os_order oo;
