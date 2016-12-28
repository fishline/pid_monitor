set mapred.job.queue.name=root.queue1;
select count(*) from store_sales where ss_list_price < 105.0 and ss_ext_list_price > 100.0;
