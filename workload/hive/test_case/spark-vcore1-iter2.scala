import org.apache.spark.sql.hive.HiveContext
val sqlContext = new HiveContext(sc)
sqlContext.sql(s"USE tpcds_bin_partitioned_orc_1000")
val result=sqlContext.sql(s"select count(*) from tpcds_bin_partitioned_orc_1000.web_sales")
result.show()
val result=sqlContext.sql(s"select count(*) from tpcds_bin_partitioned_orc_1000.web_sales")
result.show()
sc.stop()
exit()
