package src.main.scala
import java.io._
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

object EventLogParsing extends Logging {

    def main(args: Array[String]) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

        if (args.length < 1) {
            println("Usage: <path_to_spark_eventlog>")
            System.exit(0)
        }

        val conf = new SparkConf
        conf.setAppName("Analyze spark event log")
        val sc = new SparkContext(conf)
        val sqlContext: SQLContext = new SQLContext(sc)

        val path = args(0)
        val logDF = sqlContext.read.json(path)
        //logDF.printSchema()
        //val filePath = "/tmp/eventlog_schema.out"
        //new PrintWriter(filePath) { write(logDF.schema.treeString);close}

        val taskFlattened = logDF.filter(col("Event") === "SparkListenerTaskEnd").select(col("Stage ID"), explode(col("Task Info.Accumulables")).as("Accumulables_task"))

        val stageFlattened = logDF.filter(col("Event") === "SparkListenerJobStart").select(explode(col("Stage IDs")).as("stage_id"),(col("Properties.`spark.job.description`")).as("desc"))

        stageFlattened.registerTempTable("stage")
        taskFlattened.registerTempTable("task")

        // heavy table scan
        val inputData = sqlContext.sql("""select desc, round(sum(Accumulables_task.Update)/1024/1024/1024, 2) as InputData from task,stage where Accumulables_task.name = "internal.metrics.input.bytesRead" and `Stage ID` = stage_id group by desc order by InputData desc""")

        println("Top 10 heavy table scan queries")
        inputData.show(10)

        val shuffleData = sqlContext.sql("""select desc, round(sum(Accumulables_task.Update)/1024/1024/1024, 2) as shuffleGB from task,stage where Accumulables_task.name = "internal.metrics.shuffle.write.bytesWritten" and `Stage ID` = stage_id group by desc order by shuffleGB desc """)

        println("Top 10 heavy shuffle queries")
        shuffleData.show(10)

        //GC
        val stageMetrics = logDF.filter(col("Event") === "SparkListenerTaskEnd").select(col("Stage ID"), col("Task Metrics"), col("Task Info"), col("Task End Reason"), col("Task Type"))
        stageMetrics.registerTempTable("stageM")
        val stageGC = sqlContext.sql("""select  `Stage ID`,round(sum(`Task Metrics`.`JVM GC Time`)/sum(`Task Metrics`.`Executor Run Time`), 2) as GCPercent from stageM where   group by `Stage ID` order by GCPercent desc """)
        stageGC.registerTempTable("stageGCView")
        val stageGCbyDesc = sqlContext.sql("""select desc, stage_id,GCPercent from stageGCView, stage where `Stage ID` = stage_id and desc is not null""")
        println("Top 20 heavy GC time queries")
        stageGCbyDesc.show(20)


        //shuffle read block time
        val stageFectchWait = sqlContext.sql("""select  `Stage ID`, sum(`Task Metrics`.`Shuffle Read Metrics`.`Fetch Wait Time`) as shuffleBlockTime, count(`Stage ID`) as numberOfTasks from stageM where   group by `Stage ID` order by shuffleBlockTime desc """)
        stageFectchWait.registerTempTable("stageFectchView")
        val stageFectchByDesc = sqlContext.sql("""select desc, stage_id, ROUND(shuffleBlockTime/1000, 2) as shuffleBlockPerStage, ROUND(shuffleBlockTime/1000/numberOfTasks, 2) as shuffleBlockPerTask from stageFectchView, stage where `Stage ID` = stage_id and desc is not null""")
        println("Top 10 shuffle fetch blocked queries")
        stageFectchByDesc.show(10)

        //skew data
        val taskRunningTime = sqlContext.sql("""select `Stage ID`, max(`Task Info`.`Finish Time`- `Task Info`.`Launch Time`) as longestTaskTime, avg(`Task Info`.`Finish Time`- `Task Info`.`Launch Time`) as avgTaskTime from stageM group by `Stage ID` """)
        taskRunningTime.registerTempTable("TaskTime")
        val shuffleSkew = sqlContext.sql("""select desc, stage_id, longestTaskTime, round(avgTaskTime, 2) as averageTaskTime from taskTime, stage where `Stage ID` = stage_id and desc is not null order by longestTaskTime/avgTaskTime desc """)
        println("Top 10 data skew queries")
        shuffleSkew.show(10)

        //failure task
        val failedTask =  sqlContext.sql("""select `Stage ID`, `Task Type`, `Task End Reason`.`Reason`, substring(`Task End Reason`.`Message`,1,10) as errorMessages, count(*) as numberOfFailedTasks from stageM where `Task End Reason`.`Reason` != "Success" and `Task End Reason`.`Reason` != "Resubmitted" group by `Stage ID`,  `Task Type`, `Task End Reason`.`Reason`, substring(`Task End Reason`.`Message`,1,10)""")
        println("failed tasks by stage and failure reason")
        failedTask.show(100)

        //executor info
        val executor = logDF.filter(col("Event") === "SparkListenerExecutorAdded").select("Executor ID","Executor Info.Host").orderBy("Host")
        sc.stop()
    }
}
