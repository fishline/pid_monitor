package src.main.scala

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkQuery2 {

    def main(args: Array[String]) {
        if (args.length != 1) {
            println("Usage: <DB_NAME>")
            System.exit(1)
        }
        val sparkConf = new SparkConf().setAppName("SparkQuery2")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new HiveContext(sc)
        val dbName = args(0)
        sqlContext.sql(s"USE " + dbName)
        val result=sqlContext.sql(s"select count(*) from os_order where oid > 1000")
        result.show()
        sc.stop()
        System.exit(0)
    }
}
