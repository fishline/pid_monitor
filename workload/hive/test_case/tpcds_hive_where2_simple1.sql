set mapred.job.queue.name=root.hive_queue.simple1;
select count(*) from catalog_sales where cs_list_price < 105.0 and cs_ext_list_price > 100.0;
