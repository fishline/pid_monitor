set mapred.job.queue.name=root.hive_queue.simple2;
select count(*) from store_sales where ss_list_price < 105.0 and ss_ext_list_price > 100.0;
