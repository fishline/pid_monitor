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

object ContainerErrorAnalyzerV2 extends Logging {
    def main(args: Array[String]) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

        if (args.length < 5) {
            println("Usage: <path to failed container log> <path to success container log> <container gc info> <levenshtein str len> <output file>")
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

        val frdd = sc.textFile(args(0), 300).distinct().filter(x => x.contains("###NO_SUCH_BREAK###")).map{ line =>
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

        val srdd = sc.textFile(args(1), 2000).map{ line =>
            val data = line.split("###NO_SUCH_BREAK###")
                var strlen = data(2).length()
                if (strlen > lev_str_len) {
                    strlen = lev_str_len
                }
            (data(0), data(1), data(2) take (strlen - 1) mkString)
        }.toDF("tag", "orig", "fmt").select("fmt").rdd.flatMap{ case Row(f:String) =>
            var items = new ListBuffer[String]()
                items += f + "##TAG##S"
                items.toList
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
            val f = aggregateMsg(trdd)

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

            val failInfo = sc.textFile(args(0), 300).distinct().filter(x => x.contains("###NO_SUCH_BREAK###")).mapPartitions{ iter =>
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
                                    val entryKey = keyArray(0) + ":" + keyArray(3) + ":" + keyArray(2)
                                    val entry = (entryKey, keyArray(4).toInt, errMapB.value(err))
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

        val timeInfo = sc.textFile(args(0), 300).distinct().filter(x => ! x.contains("###NO_SUCH_BREAK###")).map{ line =>
            val data = line.split(":")
                val info = (data(1), data(4).toInt)
                (data(0) + ":" + data(3) + ":" + data(2), info)
        }

        val failInfoWithTime = timeInfo.leftOuterJoin(failInfoFinal).map{ case (a, b) =>
            val entry = (b._1, b._2.getOrElse(new ArrayBuffer[Int]()))
                (a, entry)
        }

        val gcInfo = sc.textFile(args(2), 100).map{line =>
            val data = line.split(" ")
                (data(0), data(1))
        }

        val failInfoAll = failInfoWithTime.leftOuterJoin(gcInfo).map{ case (a, b) =>
            val data = a.split(":")
                (data(0), data(1), data(2), b._1._1._1, b._1._1._2, b._2.getOrElse(-1), b._1._2)
        }.cache()


        failInfoAll.collect().foreach{ x =>
            var errors = "NA"
            for (i <- 0 until x._7.length) {
                if (i == 0) {
                    errors = x._7(i).toString
                } else {
                    errors = errors + "," + x._7(i)
                }
            }
            writer.write(x._1 + ";" + x._2 + ";" + x._3 + ";" + x._4 + ";" + x._5 + ";" + x._6 + ";" + x._7.length + ";" + errors + ";" + "\n")
        }
        writer.close()

        val failString = sc.textFile(args(0), 300).distinct().filter(x => x.contains("###NO_SUCH_BREAK###")).map{ line =>
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
        sc.stop()
    }
}
