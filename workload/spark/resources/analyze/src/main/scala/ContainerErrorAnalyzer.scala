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

object ContainerErrorAnalyzer extends Logging {
    def main(args: Array[String]) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

        if (args.length < 3) {
            println("Usage: <path_to_failed_container_log> <path_to_success_container_log> <output file>")
            System.exit(0)
        }

        val conf = new SparkConf
        conf.setAppName("Analyze spark event log")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        import sqlContext.implicits._

        val frdd = sc.textFile(args(0), 20).map{ line =>
            val data = line.split("###NO_SUCH_BREAK###")
                var strlen = data(2).length()
                if (strlen > 200) {
                    strlen = 200
                }
            (data(0), data(1), data(2) take (strlen - 1) mkString)
        }.toDF("tag", "orig", "fmt").select("fmt").rdd.flatMap{ case Row(f:String) =>
            var items = new ListBuffer[String]()
                items += f + "##TAG##F"
                items.toList
        }.distinct().coalesce(20, true).cache()

        val srdd = sc.textFile(args(1), 20000).map{ line =>
            val data = line.split("###NO_SUCH_BREAK###")
                var strlen = data(2).length()
                if (strlen > 200) {
                    strlen = 200
                }
            (data(0), data(1), data(2) take (strlen - 1) mkString)
        }.toDF("tag", "orig", "fmt").select("fmt").rdd.flatMap{ case Row(f:String) =>
            var items = new ListBuffer[String]()
                items += f + "##TAG##S"
                items.toList
        }.distinct().coalesce(20000, true).cache()

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

                psize = psize / 10
                    while (psize >= 10) {
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
                        psize = psize / 10
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
                var psize = input.partitions.size / 10
                while (psize >= 10) {
                    var tmpRddIn = tmpRddOut._1.coalesce(psize, true)
                        tmpRddOut = RemoveCommonThreshold(tmpRddIn)
                        outSet = merger(finalMerger(outSet), finalMerger(tmpRddOut._2))
                        psize = psize / 10
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

        val failRdd = sc.textFile(args(0), 20).map{ line =>
            val data = line.split("###NO_SUCH_BREAK###")
                val containerId = data(0).split(":")
                var strlen = data(2).length()
                if (strlen > 200) {
                    strlen = 200
                }
            (containerId(0), data(1), data(2) take (strlen - 1) mkString)
        }.cache()

        val failInfo = failRdd.collect()

        var errMapping:ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]()
            for (i <- 0 until err.length) {
                var tgt:String = ""
                    for (j <- 0 until failInfo.length) {
                        if (failInfo(j)._3 == err(i)(0)) {
                            tgt = failInfo(j)._2
                        }
                    }
                val entry = ("ERROR" + i, tgt)
                    errMapping += entry
            }

        var container_error_raw:ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]()
            for (i <- 0 until failInfo.length) {
                var errorIdx:Int = -1
                    for (j <- 0 until err.length) {
                        if (err(j).contains(failInfo(i)._3)) {
                            val entry = (failInfo(i)._1, "ERROR" + j)
                                container_error_raw += entry
                        }
                    }
            }

        var container_error:ArrayBuffer[(String, ArrayBuffer[String])] = new ArrayBuffer[(String, ArrayBuffer[String])]()
            for (i <- 0 until container_error_raw.length) {
                var exist_idx = -1
                    for (j <- 0 until container_error.length) {
                        if (container_error(j)._1 == container_error_raw(i)._1) {
                            exist_idx = j
                        }
                    }
                if (exist_idx == -1) {
                    var element = new ArrayBuffer[String]()
                        element += container_error_raw(i)._2
                        val entry = (container_error_raw(i)._1, element)
                        container_error += entry
                } else {
                    if (! container_error(exist_idx)._2.contains(container_error_raw(i)._2)) {
                        container_error(exist_idx)._2 += container_error_raw(i)._2
                    }
                }
            }

        var error_container_raw:ArrayBuffer[(String, ArrayBuffer[String])] = new ArrayBuffer[(String, ArrayBuffer[String])]
        for (i <- 0 until container_error.length) {
            for (j <- 0 until container_error(i)._2.length) {
                var exist_idx = -1
                    for (k <- 0 until error_container_raw.length) {
                        if (error_container_raw(k)._1 == container_error(i)._2(j)) {
                            exist_idx = k
                        }
                    }
                if (exist_idx == -1) {
                    var element = new ArrayBuffer[String]()
                        element += container_error(i)._1
                        var entry = (container_error(i)._2(j), element)
                        error_container_raw += entry
                } else {
                    error_container_raw(exist_idx)._2 += container_error(i)._1
                }
            }
        }

        var error_container:ArrayBuffer[(String, Int)] = new ArrayBuffer[(String, Int)]
        for (i <- 0 until error_container_raw.length) {
            val entry = (error_container_raw(i)._1, error_container_raw(i)._2.length)
                error_container += entry
        }

        val writer = new PrintWriter(new File(args(2)))

        writer.write("============= Error Mapping =============\n")
        errMapping.foreach{ x =>
            writer.write(x._1 + "\n")
            writer.write(x._2 + "\n\n")
        }
        writer.write("\n============= Container - Error =============\n")
        container_error.foreach{ x =>
            writer.write(x._1 + "    ")
                for (i <- 0 until x._2.length) {
                    writer.write(x._2(i) + ", ")
                }
            writer.write("\n")
        }
        writer.write("\n============= Error - Count =============\n")
        error_container.foreach{ x =>
            writer.write(x._1 + "    " + x._2 + "\n")
        }
        writer.close()
    }
}
