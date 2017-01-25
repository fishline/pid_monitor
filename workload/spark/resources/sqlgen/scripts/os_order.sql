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
stored as orc;

insert overwrite table os_order 
select
        oo.oid,
        oo.bid,
        oo.day,
        oo.t,
        oo.dummy
      from ${SOURCE}.os_order oo;
