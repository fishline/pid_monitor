package src.main.scala

import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._
import scala.reflect._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.text.SimpleDateFormat
import java.text.ParseException
import java.util.TimeZone
import java.util.GregorianCalendar

object ContainerErrorAnalyzerV3 extends Logging {
    def main(args: Array[String]) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
            Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

            if (args.length < 5) {
                println("Usage: <path to hottub_info seqFile> <path to daily data/sdr seqFile> <path to success container log> <levenshtein str len> <output file>")
                    System.exit(0)
            }

        val conf = new SparkConf
            conf.setAppName("Analyze spark event log")
            val sc = new SparkContext(conf)
            val sqlContext = new SQLContext(sc)
            val lev_str_len = args(3).toInt
            val writer = new PrintWriter(new File(args(4)))
            val writerErr = new PrintWriter(new File(args(4) + ".err.log"))

            import sqlContext.implicits._

            val seqInput = sc.sequenceFile(args(0), classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.BytesWritable], 500)
            val input = seqInput.map{x =>
                (x._1.toString, new String(x._2.copyBytes()))
            }.cache()

        val inputFail = input.filter{case (x, y) => x.contains("fail")}.flatMap{ case (fn, fileContent) =>
            val lines = fileContent.split("\n")
                var items = new ListBuffer[(String, String)]()
                val cid = "(container_[0-9]+_[0-9]+_[0-9]+_[0-9]+)".r
                val mi = cid.findAllIn(fn)
                if (mi.hasNext) {
                    val containerID = mi.next()
                        val fnFields = fn.split(containerID + "/")
                        val fileName = fnFields(1)
                        var wType = "NA"
                        if (fileName == "syslog") {
                            var f1 = false
                                var f2 = false
                                for (i <- 0 until lines.length) {
                                    if (lines(i).contains("MapTask metrics system")) {
                                        f1 = true
                                    } else if (lines(i).contains("org.apache.hadoop.mapred.MapTask")) {
                                        f1 = true
                                    } else if (lines(i).contains("ReduceTask metrics system")) {
                                        f2 = true
                                    } else if (lines(i).contains("org.apache.hadoop.mapreduce.task.reduce")) {
                                        f2 = true
                                    }
                                }
                            if (lines.length == 1 && lines(0).length == 0) {
                                wType = "MR-zero"
                            } else if (f1 == true) {
                                wType = "MR-map"
                            } else if (f2 == true) {
                                wType = "MR-reduce"
                            } else {
                                wType = "MR"
                            }
                        }
                    val hinfo = "(hottub_info-tdw-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+)".r
                        val hi = hinfo.findAllIn(fn)
                        if (hi.hasNext) {
                            val hottubInfo = hi.next()
                                val hottubFields = hottubInfo.split("-")
                                val date = hottubFields(7) + hottubFields(8)
                                val ip = hottubFields(2) + "." + hottubFields(3) + "." + hottubFields(4) + "." + hottubFields(5)
                                var lastLine = ""
                                var lastLineIdx = 0
                                var matchTgt = false
                                var startTS = 0L
                                var stopTS = 0L
                                val tsStr = "^([0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+)".r
                                val tgtStr = "(\\s+ERROR\\s+)|(\\s+WARN\\s+)|(\\s+FATAL\\s+)|(executor\\p{Punct}Executor\\p{Punct}\\s+Executor\\s+killed\\s+task)|(executor\\p{Punct}Executor\\p{Punct}\\s+Executor\\s+is\\s+trying\\s+to\\s+kill\\s+task)".r
                                for (i <- 0 until lines.length) {
                                    val tsIter = tsStr.findAllIn(lines(i))
                                        if (tsIter.hasNext) {
                                            // Get and update TS
                                            var format = "yyyy-MM-dd HH:mm:ss"
                                                var sdf = new SimpleDateFormat(format)
                                                val testStr = tsIter.next()
                                                try {
                                                    var d = sdf.parse(testStr)
                                                        if (d != null) {
                                                            if (startTS == 0L) {
                                                                startTS = d.getTime()
                                                            }
                                                            stopTS = d.getTime()
                                                        }
                                                } catch {
                                                    case ex: ParseException => {
                                                                 format = "yy/MM/dd HH:mm:ss"
                                                                     sdf = new SimpleDateFormat(format)
                                                                     try {
                                                                         var d = sdf.parse(testStr)
                                                                             if (d != null) {
                                                                                 if (startTS == 0L) {
                                                                                     startTS = d.getTime()
                                                                                 }
                                                                                 stopTS = d.getTime()
                                                                             }
                                                                     } catch {
                                                                         case ex1: ParseException => {}
                                                                     }
                                                             }
                                                }
                                        }
                                }
                            val duration = (stopTS - startTS) / 1000L

                                if (fileName == "stdout") {
                                    for (i <- 0 until lines.length) {
                                        if (lines(i).contains("java.lang.OutOfMemoryError")) {
                                            val tupleItem = (containerID + ":" + fileName + ":" + wType + ":" + ip + ":" + i + ":" + duration, lines(i) + "###NO_SUCH_BREAK###" + lines(i))
                                                items += tupleItem
                                        }
                                    }
                                } else {
                                    if (wType == "MR-zero") {
                                        val tupleItem = (containerID + ":" + fileName + ":" + wType + ":" + ip + ":" + "0" + ":" + "0", "")
                                            items += tupleItem
                                    } else {
                                        for (i <- 0 until lines.length) {
                                            val tsIter = tsStr.findAllIn(lines(i))
                                                if (tsIter.hasNext) {
                                                    // Found beginning of new block
                                                    if (matchTgt == true && lastLine.length() > 0) {
                                                        var lineTF = new String(lastLine)
                                                            val r1 = "^[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+".r
                                                            lineTF = r1.replaceFirstIn(lineTF, "")
                                                            val r2 = "^[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+".r
                                                            lineTF = r2.replaceFirstIn(lineTF, "")
                                                            val r3 = "[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+".r
                                                            lineTF = r3.replaceAllIn(lineTF, "")
                                                            val r4 = "[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+".r
                                                            lineTF = r4.replaceAllIn(lineTF, "")
                                                            val r5 = "job[0-9_]+".r
                                                            lineTF = r5.replaceAllIn(lineTF, "")
                                                            val r6 = "hdfs:\\S+".r
                                                            lineTF = r6.replaceAllIn(lineTF, "")
                                                            val r7 = "tdw[\\-0-9]+".r
                                                            lineTF = r7.replaceAllIn(lineTF, "")
                                                            val r8 = "attempt[0-9_mr]+".r
                                                            lineTF = r8.replaceAllIn(lineTF, "attempt_id")
                                                            val r9 = "container[0-9_]+".r
                                                            lineTF = r9.replaceAllIn(lineTF, "container_id")
                                                            val r10 = "shuffle[0-9_]+".r
                                                            lineTF = r10.replaceAllIn(lineTF, "shuffle")
                                                            val r11 = "rec\\p{Punct}[0-9]+".r
                                                            lineTF = r11.replaceAllIn(lineTF, "rec")
                                                            val r12 = "task\\s[0-9\\.]+".r
                                                            lineTF = r12.replaceAllIn(lineTF, "task")
                                                            val r13 = "stage\\s[0-9\\.]+".r
                                                            lineTF = r13.replaceAllIn(lineTF, "stage")
                                                            val r14 = "TID\\s[0-9]+".r
                                                            lineTF = r14.replaceAllIn(lineTF, "TID")
                                                            val r15 = "size\\s+=\\s+[0-9]+".r
                                                            lineTF = r15.replaceAllIn(lineTF, "size")
                                                            val r16 = "TID\\s+=\\s+[0-9]+".r
                                                            lineTF = r16.replaceAllIn(lineTF, "TID")
                                                            val r17 = "\\p{Punct}tmp[\\/0-9a-zA-Z\\._\\-]+".r
                                                            lineTF = r17.replaceAllIn(lineTF, "")
                                                            val r18 = "\\p{Punct}data[\\/0-9a-zA-Z\\._\\-]+".r
                                                            lineTF = r18.replaceAllIn(lineTF, "")
                                                            val r19 = "\\p{Punct}usr[\\/0-9a-zA-Z\\._\\-]+".r
                                                            lineTF = r19.replaceAllIn(lineTF, "")
                                                            val r20 = "\\p{Punct}stage[\\/0-9a-zA-Z\\._\\-]+".r
                                                            lineTF = r20.replaceAllIn(lineTF, "")
                                                            val r21 = "\\p{Punct}[0-9a-f]+".r
                                                            lineTF = r21.replaceAllIn(lineTF, "")
                                                            val r22 = "[a-zA-Z0-9\\-\\.]+\\p{Punct}[0-9]+".r
                                                            lineTF = r22.replaceAllIn(lineTF, "")
                                                            val r23 = "Thread-[0-9]+".r
                                                            lineTF = r23.replaceAllIn(lineTF, "Thread")
                                                            val r24 = "pool-[0-9]+-thread-[0-9]+".r
                                                            lineTF = r24.replaceAllIn(lineTF, "pool-thread")
                                                            val r25 = "\\sat\\s+".r
                                                            lineTF = r25.replaceAllIn(lineTF, "")
                                                            val r26 = "[0-9]".r
                                                            lineTF = r26.replaceAllIn(lineTF, "")
                                                            val r27 = "\\p{Punct}".r
                                                            lineTF = r27.replaceAllIn(lineTF, "")
                                                            val r28 = "\\s+".r
                                                            lineTF = r28.replaceAllIn(lineTF, " ")
                                                            val tupleItem = (containerID + ":" + fileName + ":" + wType + ":" + ip + ":" + lastLineIdx + ":" + duration, lastLine + "###NO_SUCH_BREAK###" + lineTF.toLowerCase())
                                                            items += tupleItem
                                                    }
                                                    lastLine = ""
                                                        lastLineIdx = 0
                                                        matchTgt = false
                                                        val tgtIter = tgtStr.findAllIn(lines(i))
                                                        if (tgtIter.hasNext) {
                                                            matchTgt = true
                                                                lastLine = lines(i)
                                                                lastLineIdx = i
                                                        }
                                                } else {
                                                    if (matchTgt == true) {
                                                        lastLine = lastLine + " " + lines(i)
                                                    }
                                                }
                                        }
                                        if (matchTgt == true && lastLine.length() > 0) {
                                            var lineTF = new String(lastLine)
                                                val r1 = "^[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+".r
                                                lineTF = r1.replaceFirstIn(lineTF, "")
                                                val r2 = "^[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+".r
                                                lineTF = r2.replaceFirstIn(lineTF, "")
                                                val r3 = "[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+".r
                                                lineTF = r3.replaceAllIn(lineTF, "")
                                                val r4 = "[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+".r
                                                lineTF = r4.replaceAllIn(lineTF, "")
                                                val r5 = "job[0-9_]+".r
                                                lineTF = r5.replaceAllIn(lineTF, "")
                                                val r6 = "hdfs:\\S+".r
                                                lineTF = r6.replaceAllIn(lineTF, "")
                                                val r7 = "tdw[\\-0-9]+".r
                                                lineTF = r7.replaceAllIn(lineTF, "")
                                                val r8 = "attempt[0-9_mr]+".r
                                                lineTF = r8.replaceAllIn(lineTF, "attempt_id")
                                                val r9 = "container[0-9_]+".r
                                                lineTF = r9.replaceAllIn(lineTF, "container_id")
                                                val r10 = "shuffle[0-9_]+".r
                                                lineTF = r10.replaceAllIn(lineTF, "shuffle")
                                                val r11 = "rec\\p{Punct}[0-9]+".r
                                                lineTF = r11.replaceAllIn(lineTF, "rec")
                                                val r12 = "task\\s[0-9\\.]+".r
                                                lineTF = r12.replaceAllIn(lineTF, "task")
                                                val r13 = "stage\\s[0-9\\.]+".r
                                                lineTF = r13.replaceAllIn(lineTF, "stage")
                                                val r14 = "TID\\s[0-9]+".r
                                                lineTF = r14.replaceAllIn(lineTF, "TID")
                                                val r15 = "size\\s+=\\s+[0-9]+".r
                                                lineTF = r15.replaceAllIn(lineTF, "size")
                                                val r16 = "TID\\s+=\\s+[0-9]+".r
                                                lineTF = r16.replaceAllIn(lineTF, "TID")
                                                val r17 = "\\p{Punct}tmp[\\/0-9a-zA-Z\\._\\-]+".r
                                                lineTF = r17.replaceAllIn(lineTF, "")
                                                val r18 = "\\p{Punct}data[\\/0-9a-zA-Z\\._\\-]+".r
                                                lineTF = r18.replaceAllIn(lineTF, "")
                                                val r19 = "\\p{Punct}usr[\\/0-9a-zA-Z\\._\\-]+".r
                                                lineTF = r19.replaceAllIn(lineTF, "")
                                                val r20 = "\\p{Punct}stage[\\/0-9a-zA-Z\\._\\-]+".r
                                                lineTF = r20.replaceAllIn(lineTF, "")
                                                val r21 = "\\p{Punct}[0-9a-f]+".r
                                                lineTF = r21.replaceAllIn(lineTF, "")
                                                val r22 = "[a-zA-Z0-9\\-\\.]+\\p{Punct}[0-9]+".r
                                                lineTF = r22.replaceAllIn(lineTF, "")
                                                val r23 = "Thread-[0-9]+".r
                                                lineTF = r23.replaceAllIn(lineTF, "Thread")
                                                val r24 = "pool-[0-9]+-thread-[0-9]+".r
                                                lineTF = r24.replaceAllIn(lineTF, "pool-thread")
                                                val r25 = "\\sat\\s+".r
                                                lineTF = r25.replaceAllIn(lineTF, "")
                                                val r26 = "[0-9]".r
                                                lineTF = r26.replaceAllIn(lineTF, "")
                                                val r27 = "\\p{Punct}".r
                                                lineTF = r27.replaceAllIn(lineTF, "")
                                                val r28 = "\\s+".r
                                                lineTF = r28.replaceAllIn(lineTF, " ")
                                                val tupleItem = (containerID + ":" + fileName + ":" + wType + ":" + ip + ":" + lastLineIdx + ":" + duration, lastLine + "###NO_SUCH_BREAK###" + lineTF.toLowerCase())
                                                items += tupleItem
                                        }
                                    }
                                }
                        }
                }
            items.toList
        }.cache()

        val failContainerType = inputFail.map{ x =>
            val data = x._1.split(":")
                (data(0) + ":" + data(3), (data(1), data(2), data(3), data(5)))
        }.reduceByKey{ (a, b) =>
            var duration = a._4
                if (b._4 > a._4) {
                    duration = b._4
                }
            var wType = a._2
                if (b._2 != "NA") {
                    wType = b._2
                }
            (a._1, wType, a._3, duration)
        }.map{ x =>
            val data = x._1.split(":")
                if (x._2._2 == "NA") {
                    (x._1, data(0) + ":" + "Spark" + ":" + x._2._3 + ":" + x._2._4) 
                } else {
                    (x._1, data(0) + ":" + x._2._2 + ":" + x._2._3 + ":" + x._2._4)
                }
        }

        val containerTagMap = collection.mutable.Map[String, String]()
            failContainerType.collect().foreach{ x =>
                val data = x._2.split(":")
                    containerTagMap(x._1) = data(0) + ":" + data(1) + ":" + data(2)
            }

        val containerTagMapB = sc.broadcast(containerTagMap)
            val failInfo1 = inputFail.flatMap{ x =>
                var items = new ListBuffer[String]()
                    if (x._2 != "") {
                        val data = x._1.split(":")
                            items += containerTagMapB.value(data(0) + ":" + data(3)) + ":" + data(4) + "###NO_SUCH_BREAK###" + x._2
                    }
                items.toList
            }
        val failInfo2 = failContainerType.map{ x =>
            x._2
        }
        val failInfoInput = failInfo1 ++ failInfo2
            failInfoInput.cache()


            val containerTSDuraPidWtypeHottub = input.filter{case (x, y) => x.contains("hottub_data/logs")}.flatMap{ case (fn, fileContent) =>
                val lines = fileContent.split("\n")
                    var items = new ListBuffer[(String, String)]()
                    val hinfo = "(hottub_info-tdw-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+)".r
                    val hi = hinfo.findAllIn(fn)
                    if (hi.hasNext) {
                        val hottubInfo = hi.next()
                            val hottubFields = hottubInfo.split("-")
                            val ip = hottubFields(2) + "." + hottubFields(3) + "." + hottubFields(4) + "." + hottubFields(5)
                            val ci = "(container_[0-9]+_[0-9]+_[0-9]+_[0-9]+)".r
                            var containerID = "NA"
                            var startTS = 0L
                            var duration = 0F
                            var pid = 0
                            var wType = "NA"
                            var hottubType = "CONV"
                            for (i <- 0 until lines.length) {
                                if (containerID == "NA") {
                                    val ciIter = ci.findAllIn(lines(i))
                                        if (ciIter.hasNext) {
                                            containerID = ciIter.next()
                                        }
                                }
                                if (lines(i).contains(" METRICS ")) {
                                    val metricsFields = lines(i).split(" # ")
                                        if (metricsFields.length == 14) {
                                            // Sample: @ METRICS #  CLISIGTERM #  ba6ac2b54f48f8b1bd898e747a9f4eab001024MB  # 189224063599860 # Thu Aug 10 19:48:11 2017 #   POOLREUSEJVM #      0.001 #     48.116 # 0000067474_1948 #      0000091965_1741 #    8 #    0 #    0 # YarnChild
                                            // METRICS # where # dbg_id # dbg_starttime_long # dbg_starttime_str # dbg_jvmtype # dbg_penalty_time # dbg_run_time # dbg_client_pid_str # dbg_server_pid_str # dbg_totjvm # dbg_busyjvm # dbg_unreachable_jvm # appname
                                            var format = "EEE MMM  d HH:mm:ss yyyy"
                                                var sdf = new SimpleDateFormat(format)
                                                sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
                                                try {
                                                    var d = sdf.parse(metricsFields(4))
                                                        if (d != null) {
                                                            startTS = d.getTime() / 1000L
                                                        }
                                                } catch {
                                                    case ex: ParseException => {
                                                                 format = "EEE MMM dd HH:mm:ss yyyy"
                                                                     sdf = new SimpleDateFormat(format)
                                                                     sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
                                                                     try {
                                                                         var d = sdf.parse(metricsFields(4))
                                                                             if (d != null) {
                                                                                 startTS = d.getTime() / 1000L
                                                                             }
                                                                     } catch {
                                                                         case ex1: ParseException => {}
                                                                     }
                                                             }
                                                }
                                            val dr = "^\\s+".r
                                                duration = dr.replaceFirstIn(metricsFields(7), "").toFloat
                                                if (metricsFields(5).contains("POOLNEWJVM") || metricsFields(5).contains("POOLREUSEJVM") || metricsFields(5).contains("FAILEDJVM")) {
                                                    val serverPidFields = metricsFields(9).split("_")
                                                        val sr = "^\\s+0+".r
                                                        pid = sr.replaceFirstIn(serverPidFields(0), "").toInt
                                                        hottubType = metricsFields(5)
                                                        var hr = "\\s+".r
                                                        hottubType = hr.replaceAllIn(hottubType, "")
                                                } else {
                                                    val clientPidFields = metricsFields(8).split("_")
                                                        val cr = "^\\s+0+".r
                                                        pid = cr.replaceFirstIn(clientPidFields(0), "").toInt
                                                }
                                            if (metricsFields(13).contains("YarnChild")) {
                                                wType = "MR"
                                            } else if (metricsFields(13).contains("CoarseGrainedExecutorBackend")) {
                                                wType = "Spark"
                                            } else {
                                                wType = metricsFields(13)
                                                    var wr = "^\\s+".r
                                                    wType = wr.replaceFirstIn(wType, "")
                                                    wr = "\\s+$".r
                                                    wType = wr.replaceFirstIn(wType, "")
                                            }              
                                        }
                                }
                            }
                        if (pid != 0 && containerID != "NA") {
                            val tupleItem = (containerID + ":" + ip, startTS + ":" + duration + ":" + pid + ":" + wType + ":" + hottubType)
                                items += tupleItem
                        }
                    }
                items.toList
            }.cache()

        val dailyDataInputSEQ = sc.sequenceFile(args(1), classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.BytesWritable], 50)
            val dailyDataInput = dailyDataInputSEQ.map{x =>
                (x._1.toString, new String(x._2.copyBytes()))
            }.cache()
        val pidIPTSTypeGC = dailyDataInput.filter{case (x, y) => x.contains("sdr_log")}.flatMap{ case (fn, fileContent) =>
            val lines = fileContent.split("\n")
                var items = new ListBuffer[(String, ArrayBuffer[(Int, Int)])]()
                val hinfo = "(tdw-[0-9]+-[0-9]+-[0-9]+-[0-9]+)".r
                val hi = hinfo.findAllIn(fn)
                if (hi.hasNext) {
                    val hottubInfo = hi.next()
                        val hottubFields = hottubInfo.split("-")
                        val ip = hottubFields(1) + "." + hottubFields(2) + "." + hottubFields(3) + "." + hottubFields(4)
                        var jvmRec = false
                        var currentTS = 0
                        var pidTypeTidNameMap = collection.mutable.Map[String, collection.mutable.Map[Int, String]]()
                        var pidTypeTidCPUMap = collection.mutable.Map[String, ArrayBuffer[(Int, Int)]]()
                        var pidTypeLastGCCPUMap = collection.mutable.Map[String, Int]()
                        var pidTypeActiveMap = collection.mutable.Map[String, Int]()
                        for (i <- 0 until lines.length) {
                            if (lines(i).contains("jvm_thread_utilization:")) {
                                val tsr = "[0-9]+".r
                                    val tsM = tsr.findAllIn(lines(i))
                                    if (tsM.hasNext) {
                                        currentTS = tsM.next().toInt
                                            jvmRec = true
                                    }
                            } else {
                                val endMark = "^\\s+total\\s+used\\s+free\\s+shared".r
                                    val ei = endMark.findAllIn(lines(i))
                                    if (ei.hasNext) {
                                        currentTS = 0
                                            jvmRec = false
                                            var pidTypeActiveMapBackup = collection.mutable.Map[String, Int]()
                                            // Check active map for stale pid
                                            for ((k, v) <- pidTypeActiveMap) {
                                                if (v == 0) {
                                                    // key did not show up in this cycle, consider this JVM is done
                                                    val tupleItem = (ip + ":" + k, pidTypeTidCPUMap(k))
                                                        items += tupleItem
                                                        // Cleanup three maps
                                                        pidTypeTidCPUMap -= k
                                                        pidTypeTidNameMap -= k
                                                        pidTypeLastGCCPUMap -= k
                                                } else {
                                                    pidTypeActiveMapBackup(k) = 0
                                                }
                                            }
                                        pidTypeActiveMap = pidTypeActiveMapBackup
                                    } else if ((jvmRec == true) && (lines(i).contains(" Spark") || lines(i).contains(" MR"))) {
                                        var pid = ""
                                            var wType = ""
                                            var pidGCCPU = 0
                                            var l = new String(lines(i))
                                            val pidR = "^[0-9]+".r
                                            val pidM = pidR.findAllIn(l)
                                            if (pidM.hasNext) {
                                                pid = pidM.next()
                                                    val pidD = "^[0-9]+\\s+".r
                                                    l = pidD.replaceFirstIn(l, "")
                                                    val cntR = "^[0-9]+".r
                                                    val cntM = cntR.findAllIn(l)
                                                    if (cntM.hasNext) {
                                                        val cnt = cntM.next()
                                                            val cntD = "^[0-9]+\\s+".r
                                                            l = cntD.replaceFirstIn(l, "")
                                                            val cpuR = "^[^\\s]+".r
                                                            val cpuM = cpuR.findAllIn(l)
                                                            if (cpuM.hasNext) {
                                                                val cpu = cpuM.next()
                                                                    val cpuD = "^[^\\s]+\\s+".r
                                                                    l = cpuD.replaceFirstIn(l, "")
                                                                    val typeR = "[a-zA-Z]+$".r
                                                                    val typeM = typeR.findAllIn(l)
                                                                    if (typeM.hasNext) {
                                                                        wType = typeM.next()
                                                                            // renew active map
                                                                            pidTypeActiveMap(pid + ":" + wType) = 1
                                                                            val typeD = "\\s+[a-zA-Z]+$".r
                                                                            l = typeD.replaceFirstIn(l, "")
                                                                            // Remaining piece is threadID list
                                                                            if (l != "NA") {
                                                                                val tidFields = l.split(",")
                                                                                    val cpuFields = cpu.split(",")
                                                                                    if (tidFields.length == cpuFields.length) {
                                                                                        for (j <- 0 until tidFields.length) {
                                                                                            val tidR = "^[0-9]+".r
                                                                                                val tidM = tidR.findAllIn(tidFields(j))
                                                                                                if (tidM.hasNext) {
                                                                                                    val tid = tidM.next()
                                                                                                        val tidName = tidR.replaceFirstIn(tidFields(j), "")
                                                                                                        if (tidName != "") {
                                                                                                            if (!pidTypeTidNameMap.contains(pid + ":" + wType)) {
                                                                                                                pidTypeTidNameMap(pid + ":" + wType) = collection.mutable.Map[Int, String]()
                                                                                                                    pidTypeTidNameMap(pid + ":" + wType)(tid.toInt) = tidName
                                                                                                            } else if (!pidTypeTidNameMap(pid + ":" + wType).contains(tid.toInt)) {
                                                                                                                pidTypeTidNameMap(pid + ":" + wType)(tid.toInt) = tidName
                                                                                                            }
                                                                                                        }
                                                                                                    if (pidTypeTidNameMap.contains(pid + ":" + wType) && pidTypeTidNameMap(pid + ":" + wType).contains(tid.toInt) && pidTypeTidNameMap(pid + ":" + wType)(tid.toInt).contains("ParallelGC")) {
                                                                                                        pidGCCPU += cpuFields(j).toInt
                                                                                                    }    
                                                                                                }
                                                                                        }
                                                                                    }
                                                                            }
                                                                    }
                                                            }
                                                    }
                                            }
                                        if (pid != "" && wType != "") {
                                            if (!pidTypeTidCPUMap.contains(pid + ":" + wType)) {
                                                pidTypeTidCPUMap(pid + ":" + wType) = new ArrayBuffer[(Int, Int)]
                                                    val elem = (currentTS, pidGCCPU)
                                                    pidTypeTidCPUMap(pid + ":" + wType) += elem
                                            } else {
                                                val elem = (currentTS, pidTypeLastGCCPUMap(pid + ":" + wType) + pidGCCPU)
                                                    pidTypeTidCPUMap(pid + ":" + wType) += elem
                                            }
                                            if (!pidTypeLastGCCPUMap.contains(pid + ":" + wType)) {
                                                pidTypeLastGCCPUMap(pid + ":" + wType) = pidGCCPU
                                            } else {
                                                pidTypeLastGCCPUMap(pid + ":" + wType) += pidGCCPU
                                            }
                                        }
                                    }
                            }
                        }
                }
            items.toList
        }.cache()

        val cInfo = containerTSDuraPidWtypeHottub.map{ x =>
            // (containerID + ":" + ip, startTS + ":" + duration + ":" + pid + ":" + wType + ":" + hottubType)
            val data1 = x._1.split(":")
                val data2 = x._2.split(":")
                var value = new ArrayBuffer[(String, String, String)]
                val out = (data2(0) + ":" + data2(1), x._1, data2(4))
                value += out
                (data1(1) + ":" + data2(2) + ":" + data2(3), value)
        }

        val cInfoReduced = cInfo.reduceByKey{ (a, b) =>
            var out = new ArrayBuffer[(String, String, String)]
                for (i <- 0 until a.length) {
                    out += a(i)
                }
            for (i <- 0 until b.length) {
                out += b(i)
            }
            out
        }

        val gcInfoReduced = pidIPTSTypeGC.map{ x =>
            var out = new ArrayBuffer[ArrayBuffer[(Int, Int)]]
                out += x._2
                (x._1, out)
        }.reduceByKey{ (a, b) =>
            var out = new ArrayBuffer[ArrayBuffer[(Int, Int)]]
                for (i <- 0 until a.length) {
                    out += a(i)
                }
            for (i <- 0 until b.length) {
                out += b(i)
            }
            out
        }

        val cInfoWithGC = cInfoReduced.leftOuterJoin(gcInfoReduced).flatMap{ case (a, b) =>
            var out = new ArrayBuffer[(String, String)]
                val c = b._1
                val gc = b._2.getOrElse(new ArrayBuffer[ArrayBuffer[(Int, Int)]])
                for (i <- 0 until c.length) {
                    if (c(i)._3.contains("CONV")) {
                        val data1 = c(i)._1.split(":")
                        val startTS = data1(0).toInt
                            for (j <- 0 until gc.length) {
                                if (gc(j).length > 0) {
                                    val beginTS = gc(j)(0)._1
                                        val endTS = gc(j)(gc(j).length - 1)._1 + 180    // Add 3 mins margine
                                        if (startTS >= beginTS && startTS <= endTS) {
                                            val gcDelta = gc(j)(gc(j).length - 1)._2 - gc(j)(0)._2
                                                val elem = (c(i)._2, gcDelta.toString)
                                                out += elem
                                        }
                                }
                            }
                    } else {
                        val data1 = c(i)._1.split(":")
                            val startTS = data1(0).toInt
                            val stopTS = data1(0).toInt + data1(1).toFloat.toInt
                            for (j <- 0 until gc.length) {
                                if (gc(j).length > 0) {
                                    val beginTS = gc(j)(0)._1
                                        val endTS = gc(j)(gc(j).length - 1)._1 + 180    // Add 3 mins margine
                                        if (stopTS >= beginTS && stopTS <= endTS) {
                                            // This container is ended in this period, check gc
                                            var endIdx = gc(j).length - 1
                                                while (endIdx >= 0 && gc(j)(endIdx)._1 > stopTS) {
                                                    endIdx -= 1
                                                }
                                            if (endIdx < 0) {
                                                endIdx = 0
                                            }
                                            var beginIdx = 0
                                                while (beginIdx < gc(j).length && gc(j)(beginIdx)._1 < startTS) {
                                                    beginIdx += 1
                                                }
                                            if (beginIdx >= gc(j).length) {
                                                beginIdx = gc(j).length - 1
                                            }
                                            if (beginIdx > endIdx) {
                                                beginIdx = endIdx
                                            }
                                            val gcDelta = gc(j)(endIdx)._2 - gc(j)(beginIdx)._2
                                                val elem = (c(i)._2, gcDelta.toString)
                                                out += elem
                                        }
                                }
                            }
                    }
                }
            out.toList
        }.map{ x =>
            x._1 + " " + x._2
        }

        val frdd = failInfoInput.filter(x => x.contains("###NO_SUCH_BREAK###")).map{ line =>
            val data = line.split("###NO_SUCH_BREAK###")
                var strlen = data(2).length()
                if (strlen > lev_str_len) {
                    strlen = lev_str_len
                }
            (data(0), data(1), data(2) take (strlen - 1) mkString)
        }.toDF("tag", "orig", "fmt").select("fmt").rdd.flatMap{ case Row(f:String) =>
            var items = new ListBuffer[String]()
                items += f + "##TAG##F"
                items.toList
        }.distinct().coalesce(300, true)

        val srdd = sc.textFile(args(2), 2000).map{ line =>
            val data = line.split("###NO_SUCH_BREAK###")
                data(1)
        }.map{ x =>
            var lineTF = new String(x)
                val r1 = "^[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+".r
                lineTF = r1.replaceFirstIn(lineTF, "")
                val r2 = "^[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\s+".r
                lineTF = r2.replaceFirstIn(lineTF, "")
                val r3 = "[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+".r
                lineTF = r3.replaceAllIn(lineTF, "")
                val r4 = "[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+\\p{Punct}[0-9]+".r
                lineTF = r4.replaceAllIn(lineTF, "")
                val r5 = "job[0-9_]+".r
                lineTF = r5.replaceAllIn(lineTF, "")
                val r6 = "hdfs:\\S+".r
                lineTF = r6.replaceAllIn(lineTF, "")
                val r7 = "tdw[\\-0-9]+".r
                lineTF = r7.replaceAllIn(lineTF, "")
                val r8 = "attempt[0-9_mr]+".r
                lineTF = r8.replaceAllIn(lineTF, "attempt_id")
                val r9 = "container[0-9_]+".r
                lineTF = r9.replaceAllIn(lineTF, "container_id")
                val r10 = "shuffle[0-9_]+".r
                lineTF = r10.replaceAllIn(lineTF, "shuffle")
                val r11 = "rec\\p{Punct}[0-9]+".r
                lineTF = r11.replaceAllIn(lineTF, "rec")
                val r12 = "task\\s[0-9\\.]+".r
                lineTF = r12.replaceAllIn(lineTF, "task")
                val r13 = "stage\\s[0-9\\.]+".r
                lineTF = r13.replaceAllIn(lineTF, "stage")
                val r14 = "TID\\s[0-9]+".r
                lineTF = r14.replaceAllIn(lineTF, "TID")
                val r15 = "size\\s+=\\s+[0-9]+".r
                lineTF = r15.replaceAllIn(lineTF, "size")
                val r16 = "TID\\s+=\\s+[0-9]+".r
                lineTF = r16.replaceAllIn(lineTF, "TID")
                val r17 = "\\p{Punct}tmp[\\/0-9a-zA-Z\\._\\-]+".r
                lineTF = r17.replaceAllIn(lineTF, "")
                val r18 = "\\p{Punct}data[\\/0-9a-zA-Z\\._\\-]+".r
                lineTF = r18.replaceAllIn(lineTF, "")
                val r19 = "\\p{Punct}usr[\\/0-9a-zA-Z\\._\\-]+".r
                lineTF = r19.replaceAllIn(lineTF, "")
                val r20 = "\\p{Punct}stage[\\/0-9a-zA-Z\\._\\-]+".r
                lineTF = r20.replaceAllIn(lineTF, "")
                val r21 = "\\p{Punct}[0-9a-f]+".r
                lineTF = r21.replaceAllIn(lineTF, "")
                val r22 = "[a-zA-Z0-9\\-\\.]+\\p{Punct}[0-9]+".r
                lineTF = r22.replaceAllIn(lineTF, "")
                val r23 = "Thread-[0-9]+".r
                lineTF = r23.replaceAllIn(lineTF, "Thread")
                val r24 = "pool-[0-9]+-thread-[0-9]+".r
                lineTF = r24.replaceAllIn(lineTF, "pool-thread")
                val r25 = "\\sat\\s+".r
                lineTF = r25.replaceAllIn(lineTF, "")
                val r26 = "[0-9]".r
                lineTF = r26.replaceAllIn(lineTF, "")
                val r27 = "\\p{Punct}".r
                lineTF = r27.replaceAllIn(lineTF, "")
                val r28 = "\\s+".r
                lineTF = r28.replaceAllIn(lineTF, " ")
                val strTmp = lineTF.toLowerCase()
                var strlen = strTmp.length()
                if (strlen > lev_str_len) {
                    strlen = lev_str_len
                }
            strTmp take (strlen - 1) mkString
        }.map{x =>
            x + "##TAG##S"
        }.distinct().coalesce(2000, true)

        def aggregateMsg(input:RDD[String]): ArrayBuffer[ArrayBuffer[String]] = {
            def RemoveCommonThreshold(input:RDD[String]) : (RDD[String], ArrayBuffer[ArrayBuffer[String]]) = {
                var psize = input.partitions.size    
                    val rdd1 = input.mapPartitions(iter => {
                            val vals = iter.toSeq
                            for {
                            i <- vals.toIterator
                            j <- vals
                            } yield (i, j)
                            })

                val rdd2 = rdd1.map{ case (i, j) =>
                    var flag = 0
                        if (i == j) {
                            flag = 0
                        } else {
                            if ((i.length() <= j.length()) && ((j.length() - i.length()) <= i.length()/5)) {
                                flag = 1
                            } else {
                                flag = 0
                            }
                        }
                    (i, j, flag)
                }

                val rdd3 = rdd2.filter(x => x._3 == 1)
                    val df4 = rdd3.toDF("i", "j", "k")
                    val df5 = df4.withColumn("distance", levenshtein(df4("i"), df4("j")))
                    val df6 = df5.withColumn("length", length(df5("i")))
                    val df7 = df6.withColumn("tgt_length", expr("length / 5"))
                    val df8 = df7.filter("distance <= tgt_length")
                    val rdd9 = df8.rdd.flatMap{ case Row(i:String, j:String, k, d, l, t) =>
                        var items = new ListBuffer[(String, String)]()
                            val tup_items = (i, j)
                            items += tup_items
                            items.toList
                    }
                var resultRdd = rdd9.mapPartitions(iter => {
                        var out = new ArrayBuffer[ArrayBuffer[String]]()
                        for (elem <- iter) {
                        var set = out
                        out = new ArrayBuffer[ArrayBuffer[String]]()
                        var idx_1 = -1
                        var idx_2 = -1
                        for (i <- 0 until set.length) {
                        var match_1_idx = -1
                        var match_2_idx = -1
                        for (j <- 0 until set(i).length) {
                        if (set(i)(j) == elem._1) {
                        match_1_idx = j
                        }
                        if (set(i)(j) == elem._2) {
                        match_2_idx = j
                        }
                        }
                        if (match_1_idx == -1 && match_2_idx == -1) {
                        out += set(i)
                        } else {
                            if (match_1_idx != -1) {
                                idx_1 = i
                            }
                            if (match_2_idx != -1) {
                                idx_2 = i
                            }
                        }
                        }
                if (idx_1 != -1 && idx_2 != -1) {
                    if (idx_1 != idx_2 ) {
                        var entry = new ArrayBuffer[String]()
                            for (i <- 0 until set(idx_1).length) {
                                entry += set(idx_1)(i)
                            }
                        for (i <- 0 until set(idx_2).length) {
                            if (! entry.contains(set(idx_2)(i))) {
                                entry += set(idx_2)(i)
                            }
                        }
                        out += entry
                    } else {
                        out += set(idx_1)
                    }
                } else if (idx_1 != -1) {
                    var entry = new ArrayBuffer[String]()
                        for (i <- 0 until set(idx_1).length) {
                            entry += set(idx_1)(i)
                        }
                    entry += elem._2
                        out += entry
                } else if (idx_2 != -1) {
                    var entry = new ArrayBuffer[String]()
                        for (i <- 0 until set(idx_2).length) {
                            entry += set(idx_2)(i)
                        }
                    entry += elem._1
                        out += entry
                } else {
                    var entry = new ArrayBuffer[String]()
                        entry += elem._1
                        entry += elem._2
                        out += entry
                }
                        }
                out.toIterator
                }).cache()

                psize = psize / 5
                    while (psize > 2) {
                        resultRdd = resultRdd.coalesce(psize, true).mapPartitions(iter => {
                                val input = iter.toList
                                var doneList:ArrayBuffer[Int] = new ArrayBuffer[Int]()
                                var output:ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
                                for (i <- 0 until input.length) {
                                if (! doneList.contains(i)) {
                                var mergeList:ArrayBuffer[Int] = new ArrayBuffer[Int]()
                                mergeList += i
                                for (j <- 0 until input(i).length) {
                                for (l <- 0 until input.length) {
                                if (l != i && input(l).contains(input(i)(j)) && (! mergeList.contains(l))) {
                                mergeList += l
                                }
                                }
                                }
                                var entry:ArrayBuffer[String] = new ArrayBuffer[String]()
                                for (j <- 0 until mergeList.length) {
                                for (l <- 0 until input(mergeList(j)).length) {
                                if (! entry.contains(input(mergeList(j))(l))) {
                                entry += input(mergeList(j))(l)
                                }
                                }
                                }
                                output += entry
                                    for (j <- 0 until mergeList.length) {
                                        if (! doneList.contains(mergeList(j))) {
                                            doneList += mergeList(j)
                                        }
                                    }
                                }
                                }
                        output.toIterator
                        }).cache()
                        psize = psize / 5
                    }

                val sizeAccumulator: (ArrayBuffer[ArrayBuffer[String]], ArrayBuffer[String]) => ArrayBuffer[ArrayBuffer[String]] = { (set, input) =>
                    var set_m = 0
                        for (i <- 0 until set.length) {
                            if (set_m == 0) {
                                var elem_m = 0
                                    for (m <- 0 until input.length) {
                                        for (n <- 0 until set(i).length) {
                                            if (elem_m == 0) {
                                                if (set(i)(n) == input(m)) {
                                                    elem_m = 1
                                                        set_m = 1
                                                        for (p <- 0 until input.length) {
                                                            var found = 0
                                                                for (q <- 0 until set(i).length) {
                                                                    if (input(p) == set(i)(q)) {
                                                                        found = 1
                                                                    }
                                                                }
                                                            if (found == 0) {
                                                                set(i) += input(p)
                                                            }
                                                        }
                                                }
                                            }
                                        }
                                    }                
                            }
                        }
                    if (set_m == 0) {
                        set += input
                    }
                    set
                }

                val merger: (ArrayBuffer[ArrayBuffer[String]], ArrayBuffer[ArrayBuffer[String]]) => ArrayBuffer[ArrayBuffer[String]] = { (set1, set2) =>
                    var out = new ArrayBuffer[ArrayBuffer[String]]()
                        for (i <- 0 until set1.length) {
                            out += set1(i)
                        }
                    for (j <- 0 until set2.length) {
                        var set_m = 0
                            for (i <- 0 until set1.length) {
                                if (set_m == 0) {
                                    var elem_m = 0
                                        for (m <- 0 until set2(j).length) {
                                            for (n <- 0 until set1(i).length) {
                                                if (elem_m == 0) {
                                                    if (set1(i)(n) == set2(j)(m)) {
                                                        elem_m = 1
                                                            set_m = 1
                                                            for (p <- 0 until set2(j).length) {
                                                                var found = 0
                                                                    for (q <- 0 until set1(i).length) {
                                                                        if (set2(j)(p) == set1(i)(q)) {
                                                                            found = 1
                                                                        }
                                                                    }
                                                                if (found == 0) {
                                                                    out(i) += set2(j)(p)
                                                                }
                                                            }
                                                    }
                                                }
                                            }
                                        }                
                                }
                            }
                        if (set_m == 0) {
                            out += set2(j)
                        }
                    }
                    out
                }

                var array1:ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
                    val result = resultRdd.aggregate(array1)(sizeAccumulator, merger)
                    var removeAB:ArrayBuffer[String] = new ArrayBuffer[String]()
                    for (i <- 0 until result.length) {
                        var min_length = 100000
                            var min_idx = -1
                            for (j <- 0 until result(i).length) {
                                if (result(i)(j).length() < min_length) {
                                    min_length = result(i)(j).length()
                                        min_idx = j
                                }
                            }
                        for (j <- 0 until result(i).length) {
                            if (j != min_idx) {
                                removeAB += result(i)(j)
                            }
                        }
                    }

                val broadcastRemoveAB = sc.broadcast(removeAB)

                    val output = input.filter(x => ! broadcastRemoveAB.value.contains(x))
                    (output, result)
            } 

            val merger: (ArrayBuffer[ArrayBuffer[String]], ArrayBuffer[ArrayBuffer[String]]) => ArrayBuffer[ArrayBuffer[String]] = { (set1, set2) =>
                var out = new ArrayBuffer[ArrayBuffer[String]]()
                    for (i <- 0 until set1.length) {
                        out += set1(i)
                    }
                for (j <- 0 until set2.length) {
                    var set_m = 0
                        for (i <- 0 until set1.length) {
                            if (set_m == 0) {
                                var elem_m = 0
                                    for (m <- 0 until set2(j).length) {
                                        for (n <- 0 until set1(i).length) {
                                            if (elem_m == 0) {
                                                if (set1(i)(n) == set2(j)(m)) {
                                                    elem_m = 1
                                                        set_m = 1
                                                        for (p <- 0 until set2(j).length) {
                                                            var found = 0
                                                                for (q <- 0 until set1(i).length) {
                                                                    if (set2(j)(p) == set1(i)(q)) {
                                                                        found = 1
                                                                    }
                                                                }
                                                            if (found == 0) {
                                                                out(i) += set2(j)(p)
                                                            }
                                                        }
                                                }
                                            }
                                        }
                                    }                
                            }
                        }
                    if (set_m == 0) {
                        out += set2(j)
                    }
                }
                out
            }

            def finalMerger:ArrayBuffer[ArrayBuffer[String]] => ArrayBuffer[ArrayBuffer[String]] = { input =>
                var doneList:ArrayBuffer[Int] = new ArrayBuffer[Int]()
                    var output:ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
                    for (i <- 0 until input.length) {
                        if (! doneList.contains(i)) {
                            var mergeList:ArrayBuffer[Int] = new ArrayBuffer[Int]()
                                mergeList += i
                                for (j <- 0 until input(i).length) {
                                    for (l <- 0 until input.length) {
                                        if (l != i && input(l).contains(input(i)(j)) && (! mergeList.contains(l))) {
                                            mergeList += l
                                        }
                                    }
                                }
                            var entry:ArrayBuffer[String] = new ArrayBuffer[String]()
                                for (j <- 0 until mergeList.length) {
                                    for (l <- 0 until input(mergeList(j)).length) {
                                        if (! entry.contains(input(mergeList(j))(l))) {
                                            entry += input(mergeList(j))(l)
                                        }
                                    }
                                }
                            output += entry
                                for (j <- 0 until mergeList.length) {
                                    if (! doneList.contains(mergeList(j))) {
                                        doneList += mergeList(j)
                                    }
                                }
                        }
                    }
                output
            }

            var tmpRddOut = RemoveCommonThreshold(input)
                var outSet = tmpRddOut._2
                var psize = input.partitions.size / 5
                while (psize > 2) {
                    var tmpRddIn = tmpRddOut._1.coalesce(psize, true)
                        tmpRddOut = RemoveCommonThreshold(tmpRddIn)
                        outSet = merger(finalMerger(outSet), finalMerger(tmpRddOut._2))
                        psize = psize / 5
                }    
            tmpRddOut = RemoveCommonThreshold(tmpRddOut._1.coalesce(1, true))
                outSet = merger(finalMerger(outSet), finalMerger(tmpRddOut._2))

                val arrayAcc: (ArrayBuffer[ArrayBuffer[String]], String) => ArrayBuffer[ArrayBuffer[String]] = { (arr, input) =>
                    var out = new ArrayBuffer[ArrayBuffer[String]]()
                        for (i <- 0 until arr.length) {
                            out += arr(i)
                        }
                    var arr1 = new ArrayBuffer[String]()
                        arr1 += input
                        out += arr1
                        out
                }

            val arrayMerger: (ArrayBuffer[ArrayBuffer[String]], ArrayBuffer[ArrayBuffer[String]]) => ArrayBuffer[ArrayBuffer[String]] = { (set1, set2) =>
                var out = new ArrayBuffer[ArrayBuffer[String]]()
                    for (i <- 0 until set1.length) {
                        out += set1(i)
                    }
                for (j <- 0 until set2.length) {
                    out += set2(j)
                }
                out
            }

            var zArray:ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
                val partialResult = tmpRddOut._1.aggregate(zArray)(arrayAcc, arrayMerger)

                val fm = merger(finalMerger(outSet), finalMerger(partialResult))
                val finalResult = finalMerger(fm)
                finalResult
        }

        val trdd = srdd ++ frdd
            val fRaw = aggregateMsg(trdd)

            var fFirst = new ArrayBuffer[String]
            for (i <- 0 until fRaw.length) {
                fFirst += fRaw(i)(0)
            }

        val fFirstRdd = sc.parallelize(fFirst, 1)
            val fRaw2 = aggregateMsg(fFirstRdd)
            var f = new ArrayBuffer[ArrayBuffer[String]]
            for (i <- 0 until fRaw2.length) {
                var fElem = new ArrayBuffer[String]
                    for (j <- 0 until fRaw2(i).length) {
                        fElem += fRaw2(i)(j)
                            for (k <- 0 until fRaw.length) {
                                if (fRaw2(i)(j) == fRaw(k)(0) && fRaw(k).length > 0) {
                                    for (m <- 1 until fRaw(k).length) {
                                        fElem += fRaw(k)(m)
                                    }
                                }
                            }
                    }
                f += fElem
            }

        var err:ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
            for (i <- 0 until f.length) {
                var s_cnt = 0
                    var entry:ArrayBuffer[String] = new ArrayBuffer[String]()
                    for (j <- 0 until f(i).length) {
                        val fields = f(i)(j).split("##TAG##")
                            entry += fields(0)
                            if (fields(1).trim == "S") {
                                s_cnt = s_cnt + 1
                            }
                    }
                if (s_cnt == 0) {
                    err += entry
                }
            }

        val errMap = collection.mutable.Map[String, String]()
            for (i <- 0 until err.length) {
                for (j <- 0 until err(i).length) {
                    errMap(err(i)(j)) = "ERROR" + i
                }
            }

        val errMapB = sc.broadcast(errMap)

            val failInfo = failInfoInput.filter(x => x.contains("###NO_SUCH_BREAK###")).mapPartitions{ iter =>
                var out = new ArrayBuffer[(String, Int, String)]
                    for (elem <- iter) {
                        val data = elem.split("###NO_SUCH_BREAK###")
                            var strlen = data(2).length()
                            if (strlen > lev_str_len) {
                                strlen = lev_str_len
                            }
                        val err:String = data(2).take(strlen - 1)
                            if (errMapB.value.contains(err)) {
                                val keyArray = data(0).split(":")
                                    val entryKey = keyArray(0) + ":" + keyArray(2)
                                    val entry = (entryKey, keyArray(3).toInt, errMapB.value(err))
                                    out += entry
                            }
                    }
                out.toIterator
            }.map(x => (x._1, (x._2, x._3))).cache()

        val failInfoLast = failInfo.reduceByKey{ (a, b) =>
            if (a._1 > b._1) {
                a
            } else {
                b
            }
        }

        val failErr = failInfoLast.map{ case(a, b) =>
            (b._2, 1)
        }.reduceByKey((a, b) => a + b)

        val errCntBase = sc.parallelize(0 until err.length).flatMap{ idx =>
            var items = new ListBuffer[(String, Int)]()
                val item = ("ERROR" + idx, 0)
                items += item
                items.toList
        }

        val failCntTotalRaw = failErr ++ errCntBase
            val failCntTotal = failCntTotalRaw.reduceByKey((a, b) => a + b)

            val failCntTotalR = failCntTotal.map{ case(a, b) =>
                (b, a)
            }
        val failCntTotalSorted = failCntTotalR.sortByKey(ascending=false)

            val errSortMap = collection.mutable.Map[String, String]()
            val failCntTotalSortedData = failCntTotalSorted.collect()
            for (i <- 0 until failCntTotalSortedData.length) {
                errSortMap(failCntTotalSortedData(i)._2) = "ERROR" + i
            }
        val errSortMapR = collection.mutable.Map[String, String]()
            for (i <- 0 until failCntTotalSortedData.length) {
                errSortMapR("ERROR" + i) = failCntTotalSortedData(i)._2
            }

        val errSortMapB = sc.broadcast(errSortMap)

            val failInfoRemap = failInfo.map{ case (a, b) =>
                (a, (b._1, errSortMapB.value(b._2)))
            }.cache()

        val failInfoFinal = failInfoRemap.map{ case (a, b) =>
            var out = new ArrayBuffer[Int]()
                out += b._2.split("ERROR")(1).toInt
                (a, out)
        }.reduceByKey{ (a, b) =>
            var out = new ArrayBuffer[Int]()
                for (i <- 0 until a.length) {
                    if (! out.contains(a(i))) {
                        out += a(i)
                    }
                }
            for (i <- 0 until b.length) {
                if (! out.contains(b(i))) {
                    out += b(i)
                }
            }
            out
        }.map{ case (a, b) =>
            var b_sorted = b.sortWith(_ < _)
                (a, b_sorted)
        }.cache()

        val timeInfo = failInfoInput.filter(x => ! x.contains("###NO_SUCH_BREAK###")).map{ line =>
            val data = line.split(":")
                val info = (data(1), data(3).toInt)
                (data(0) + ":" + data(2), info)
        }

        val failInfoWithTime = timeInfo.leftOuterJoin(failInfoFinal).map{ case (a, b) =>
            val entry = (b._1, b._2.getOrElse(new ArrayBuffer[Int]()))
                (a, entry)
        }

        val gcInfo = cInfoWithGC.map{line =>
            val data = line.split(" ")
                (data(0), data(1))
        }

        val failInfoAll = failInfoWithTime.leftOuterJoin(gcInfo).map{ case (a, b) =>
            val data = a.split(":")
                (data(0), data(1), b._1._1._1, b._1._1._2, b._2.getOrElse(-1), b._1._2)
        }.cache()

        val failString = failInfoInput.filter(x => x.contains("###NO_SUCH_BREAK###")).map{ line =>
            val data = line.split("###NO_SUCH_BREAK###")
                var strlen = data(2).length()
                if (strlen > lev_str_len) {
                    strlen = lev_str_len
                }
            (data(1), data(2) take (strlen - 1) mkString)
        }.cache()

        for (i <- 0 until err.length) {
            writerErr.write("ERROR" + i + "\n")
                val e = errSortMapR("ERROR" + i)
                val data = e.split("ERROR")
                val err_tgt = err(data(1).toInt)(0)
                val realErr = failString.filter(x => x._2 == err_tgt).take(1)(0)._1
                writerErr.write(realErr + "\n\n")
        }
        writerErr.close()

            val cIPRowInfo = input.filter{case (x, y) => x.contains("rows.log")}.flatMap{ case (fn, fileContent) =>
                val lines = fileContent.split("\n")
                    var items = new ListBuffer[(String, String)]()
                    val hinfo = "(hottub_info-tdw-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+)".r
                    val hi = hinfo.findAllIn(fn)
                    if (hi.hasNext) {
                        val hottubInfo = hi.next()
                            val hottubFields = hottubInfo.split("-")
                            val ip = hottubFields(2) + "." + hottubFields(3) + "." + hottubFields(4) + "." + hottubFields(5)
                            // Check for following three regex on each line
                            val cr = "(container_[0-9]+_[0-9]+_[0-9]+_[0-9]+)".r
                            val mrr = "ExecMapper".r
                            val pr = "(processed\\s+[0-9]+\\s+rows)".r
                            for (i <- 0 until lines.length) {
                                val cm = cr.findAllIn(lines(i))
                                    if (cm.hasNext) {
                                        val cid = cm.next()
                                            var t = "MR-reduce"
                                            val mrm = mrr.findAllIn(lines(i))
                                            if (mrm.hasNext) {
                                                t = "MR-map"
                                            }
                                        val pm = pr.findAllIn(lines(i))
                                            if (pm.hasNext) {
                                                val fields = pm.next().split("\\s+")
                                                    val tupleItem = (cid + ":" + ip, fields(1) + ":" + t)
                                                    items += tupleItem
                                            }
                                    }
                            }
                    }
                items.toList
            }.cache()

        val containerTSDuraPidWtypeHottubForRed = containerTSDuraPidWtypeHottub.map{ x =>
            val data = x._2.split(":")
                (x._1, "STARTTS:" + data(0) + "#" + "DURATION_HT:" + data(1) + "#" + "WTYPE:" + data(3) + "#" + "HOTTUB:" + data(4))
        }

        val cInfoWithGCForRed = cInfoWithGC.map{ x =>
            val data = x.split("\\s+")
                (data(0), "GCCOST:" + data(1))
        }

        val cIPRowInfoForRed = cIPRowInfo.map{ x =>
            val data = x._2.split(":")
                (x._1, "ROWS:" + data(0) + "#" + "WTYPE:" + data(1))
        }

        val failInfoAllForRed = failInfoAll.map{ x =>
            var gc = "NA"
                if (x._5 != -1) {
                    gc = x._5.toString
                }
            var errs = "NONE"
                if (x._6.length > 0) {
                    errs = x._6(0).toString
                        if (x._6.length > 1) {
                            for (i <- 1 until x._6.length) {
                                errs = errs + ";" + x._6(i)
                            }
                        }
                }
            (x._1 + ":" + x._2, "WTYPE:" + x._3 + "#" + "DURATION_C:" + x._4 + "#" + "GCCOST:" + gc + "#" + "ERRS:" + errs)
        }

        val infoAll = containerTSDuraPidWtypeHottubForRed ++ cInfoWithGCForRed ++ cIPRowInfoForRed  ++ failInfoAllForRed
            val infoAllRed = infoAll.reduceByKey{ (a, b) =>
                a + "#" + b
            }

        val finalTable = infoAllRed.map{ x =>
            val data1 = x._1.split(":")
                val cid = data1(0)
                val cidFields = cid.split("_")
                val appid = "application_" + cidFields(1) + "_" + cidFields(2)
                val hostip = data1(1)
                val data2 = x._2.split("#")
                var wtype = ""
                for (i <- 0 until data2.length) {
                    if (data2(i).contains("WTYPE")) {
                        val tmpFields = data2(i).split(":")
                            if (tmpFields(1) != "NA" && tmpFields(1).length > wtype.length) {
                                wtype = tmpFields(1)
                            }
                    }
                }
            if (wtype == "") {
                wtype = "NA"
            }
            var startdate = "NA"
                for (i <- 0 until data2.length) {
                    if (data2(i).contains("STARTTS")) {
                        val tmpFields = data2(i).split(":")
                            val calendar = new GregorianCalendar
                            calendar.setTimeInMillis(tmpFields(1).toLong * 1000L)
                            calendar.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
                            val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            formatter.setCalendar(calendar)
                            startdate = formatter.format(calendar.getTime())
                    }
                }
            var startts = "-1"
                for (i <- 0 until data2.length) {
                    if (data2(i).contains("STARTTS")) {
                        val tmpFields = data2(i).split(":")
                            startts = tmpFields(1).toString
                    }
                }
            var duration_ht = ""
                for (i <- 0 until data2.length) {
                    if (data2(i).contains("DURATION_HT")) {
                        val tmpFields = data2(i).split(":")
                        duration_ht = tmpFields(1).toString
                    }
                }
            if (duration_ht == "") {
                duration_ht = "-1"
            }
            var duration_c = ""
                for (i <- 0 until data2.length) {
                    if (data2(i).contains("DURATION_C")) {
                        val tmpFields = data2(i).split(":")
                        duration_c = tmpFields(1).toString
                    }
                }
            if (duration_c == "") {
                duration_c = "-1"
            }
            var rows = "-1"
                for (i <- 0 until data2.length) {
                    if (data2(i).contains("ROWS")) {
                        val tmpFields = data2(i).split(":")
                            rows = tmpFields(1).toString
                    }
                }
            var gccost = "-1"
                for (i <- 0 until data2.length) {
                    if (data2(i).contains("GCCOST")) {
                        val tmpFields = data2(i).split(":")
                            gccost = tmpFields(1).toString
                    }
                }
            var hottub = "NA"
                for (i <- 0 until data2.length) {
                    if (data2(i).contains("HOTTUB")) {
                        val tmpFields = data2(i).split(":")
                            hottub = tmpFields(1).toString
                    }
                }
            var errs = "NONE"
                for (i <- 0 until data2.length) {
                    if (data2(i).contains("ERRS")) {
                        val tmpFields = data2(i).split(":")
                            errs = tmpFields(1).toString
                    }
                }
            cid + "," + appid + "," + hostip + "," + wtype + "," + startdate + "," + startts + "," + duration_ht + "," + duration_c + "," + rows + "," + gccost + "," + hottub + "," + errs
        }

        writer.write("ContainerID,ApplicationID,HostIP,WorkloadType,StartDATE,StartTS,DurationFromHottub,DurationFromContainerLog,Rows,GCCost,HottubJVMType,Errors\n")
        finalTable.collect().foreach{ x =>
            writer.write(x + "\n")
        }
        writer.close()

            sc.stop()
    }
}
