package src.main.scala

import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd._
import scala.reflect._
import org.apache.log4j.Logger
import org.apache.log4j.Level

case class timestamps(ts:Long, descrip:String)
case class tasks(start_ts:Long, duration_ts:Int)

object PowerNodeStatistics extends Logging {

    def main(args: Array[String]) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

        if (args.length < 6) {
            println("Usage: <TS> <MR> <SCALA> <SPARKSQL> <POWER> <Period>")
            System.exit(0)
        }

        val conf = new SparkConf
        conf.setAppName("Analyze Power node statistics")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        val period = args(5).toInt
        
        import sqlContext.implicits._

        val timestampsRdd = sc.textFile(args(0)).map{line =>
            val data=line.split("\\s+")
            var name = ""
            var idx = 0
            for (idx <- 1 to (data.length - 1)) {
                name = name + data(idx)
                if (idx != (data.length - 1)) {
                    name = name + " "
                }
            }
            timestamps(data(0).toLong, name)
        }
        timestampsRdd.toDF.registerTempTable("timestamps")

        val mrRdd = sc.textFile(args(1), 20).map{line =>
            val data=line.split("\\s+")
            tasks(data(0).toLong, data(1).toInt)
        }.toDF.registerTempTable("mr")

        val mrRdd2 = sqlContext.sql(s"select * from mr join timestamps on not (((mr.start_ts + mr.duration_ts) < timestamps.ts) or (mr.start_ts > (timestamps.ts + $period))) order by ts").toDF.registerTempTable("mr_tmp")
        val mrRdd3 = sqlContext.sql("select ts,first(descrip) as descrip, count(*) as mr_count from mr_tmp group by ts order by ts").toDF.registerTempTable("mr_result")

        val scalaRdd = sc.textFile(args(2), 20).map{line =>
            val data=line.split("\\s+")
            tasks(data(0).toLong, data(1).toInt)
        }.toDF.registerTempTable("scala")

        val scalaRdd2 = sqlContext.sql(s"select * from scala join timestamps on not (((scala.start_ts + scala.duration_ts) < timestamps.ts) or (scala.start_ts > (timestamps.ts + $period))) order by ts").toDF.registerTempTable("scala_tmp")
        val scalaRdd3 = sqlContext.sql("select ts,first(descrip) as descrip, count(*) as scala_count from scala_tmp group by ts order by ts").toDF.registerTempTable("scala_result")
        
        val spsqlRdd = sc.textFile(args(3), 20).map{line =>
            val data=line.split("\\s+")
            tasks(data(0).toLong, data(1).toInt)
        }.toDF.registerTempTable("spsql")

        val spsqlRdd2 = sqlContext.sql(s"select * from spsql join timestamps on not (((spsql.start_ts + spsql.duration_ts) < timestamps.ts) or (spsql.start_ts > (timestamps.ts + $period))) order by ts").toDF.registerTempTable("spsql_tmp")
        val spsqlRdd3 = sqlContext.sql("select ts,first(descrip) as descrip, count(*) as spsql_count from spsql_tmp group by ts order by ts").toDF.registerTempTable("spsql_result")
 
        sqlContext.sql("select mr_result.ts, mr_result.descrip, mr_result.mr_count, scala_result.scala_count from mr_result full outer join scala_result on mr_result.descrip = scala_result.descrip order by mr_result.ts").toDF.registerTempTable("result_tmp")
        sqlContext.sql("select result_tmp.ts, result_tmp.descrip, mr_count, scala_count, spsql_count from result_tmp full outer join spsql_result on result_tmp.descrip = spsql_result.descrip order by result_tmp.ts").toDF.registerTempTable("result_tmp2")

        val powerRdd = sc.textFile(args(4), 20).map{line =>
            val data=line.split("\\s+")
            tasks(data(0).toLong, data(1).toInt)
        }.toDF.registerTempTable("power")

        val powerRdd2 = sqlContext.sql(s"select ts,sum(duration_ts)/count(*) as power_avg, max(duration_ts) as power_max from power join timestamps on (power.start_ts >= timestamps.ts and power.start_ts < (timestamps.ts + $period)) group by ts order by ts").toDF.registerTempTable("power_tmp")
        sqlContext.sql("select result_tmp2.descrip, mr_count, scala_count, spsql_count, power_avg, power_max from result_tmp2 full outer join power_tmp on result_tmp2.ts = power_tmp.ts order by result_tmp2.ts").toDF.registerTempTable("result_tmp3")
        sqlContext.sql("select descrip, case when isnull(mr_count) then 0 else mr_count end as mr_count, case when isnull(scala_count) then 0 else scala_count end as scala_count, case when isnull(spsql_count) then 0 else spsql_count end as spsql_count, power_avg, power_max from result_tmp3").toDF.registerTempTable("result")
        sqlContext.sql("select * from result").show(500, false)

        sc.stop()
    }
}
