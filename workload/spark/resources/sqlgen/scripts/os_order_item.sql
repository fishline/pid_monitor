create database if not exists ${DB};
use ${DB};

drop table if exists os_order_item;

create table os_order_item
(
      iid         int               
,     oid         int              
,     gid         int                         
,     gnum        float                        
,     gprice      float                       
,     gsell       float                       
,     dummy       string                       
)
stored as orc tblproperties ("orc.compress"="NONE"); 

insert overwrite table os_order_item
select
        ooi.iid,
        ooi.oid,
        ooi.gid,
        ooi.gnum,
        ooi.gprice,
        ooi.gsell,
        ooi.dummy
      from ${SOURCE}.os_order_item ooi;
