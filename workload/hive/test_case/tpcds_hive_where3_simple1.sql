set mapred.job.queue.name=root.hive_queue.simple1;
select count(*) from web_sales where ws_list_price < 105.0 and ws_ext_list_price > 100.0;
