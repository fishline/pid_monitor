import org.apache.spark.sql.hive.HiveContext
val sqlContext = new HiveContext(sc)
sqlContext.sql(s"USE tpcds_bin_partitioned_orc_1000")
val result=sqlContext.sql(s"select count(*) from tpcds_bin_partitioned_orc_1000.catalog_sales where cs_list_price < 105.0 and cs_ext_list_price > 100.0")
result.show()
exit()
