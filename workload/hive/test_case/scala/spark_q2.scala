import org.apache.spark.sql.hive.HiveContext
val sqlContext = new HiveContext(sc)
sqlContext.sql(s"USE sql_20g")
val result=sqlContext.sql(s"select count(*) from os_order where oid > 1000")
result.show()
sc.stop()
exit()
