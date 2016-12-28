set mapred.job.queue.name=root.queue1;
select count(*) from catalog_sales where cs_list_price < 105.0 and cs_ext_list_price > 100.0;
