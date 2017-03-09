package src.main.scala
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{ SparkContext, SparkConf, Logging }
import org.apache.spark.SparkContext._
import org.apache.spark.graphx
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._
import scala.Serializable
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph.fromEdges

import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.impl.{ EdgePartitionBuilder, GraphImpl }
import org.graphframes._
import org.graphframes.lib.AggregateMessages
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.graphframes.GraphFrame.{DST, ID, SRC}
import org.apache.spark.sql.Row

object CommonFriendsGF extends Logging {
    
    object Buffer {
        def create():Buffer = {
            new Buffer(4)
        }

        def create(value:Long):Buffer = {
            var buf = new Buffer(1)
            buf += value
        }
    }

    class Buffer (initSize:Int) extends scala.Serializable {
        var curSize:Int = 0 
        var elements:Array[Long] = new Array[Long](initSize)

        def +=(value:Long):Buffer = {
            val newIdx = curSize
            grow2Size(curSize + 1)
            elements(newIdx) = value
            this
        }

        def ++=(value:Buffer):Buffer = {
            val oldSize = this.curSize
            val itsSize = value.curSize
            var itsElements = value.elements
            grow2Size(oldSize + itsSize)
            System.arraycopy(itsElements, 0, this.elements, oldSize, itsSize)
            this
        }

        def toArray(targetSize:Int):Array[Long] = {
            var newSize:Int = 0

            if (targetSize > this.curSize) {
                newSize = this.curSize
            } else {
                newSize = targetSize
            }
            val result = new Array[Long](newSize)
            System.arraycopy(this.elements, 0, result, 0, newSize)
            if (newSize == this.capacity()) {
                this.elements
            } else {
                result
            }
        }

        def length():Int = {
            return this.curSize
        }

        def size():Int = {
            return this.curSize
        }

        def capacity():Int = {
            return elements.length
        }

        def grow2Size(newSize:Int):Unit = {
            if (newSize < 0) {
                println("grow2Size does not accept negative size")
                return
            }
            if (newSize > capacity()) {
                var newArrayLen = 8;
                while (newSize > newArrayLen) {
                    newArrayLen *= 2
                    if (newArrayLen == scala.Int.MinValue) {
                        newArrayLen = 2147483645;
                    }
                }
                var newArray:Array[Long] = new Array[Long](newArrayLen)
                System.arraycopy(this.elements, 0, newArray, 0, this.elements.length)
                this.elements = newArray
            }
            this.curSize = newSize
        }
    }

    def main(args: Array[String]) {
        if (args.length < 2) {
            println("usage: <input> <output>")
                System.exit(0)
        }
        //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        //Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

        var partitionNum:Int = 100
        if (args.length >= 3) {
            partitionNum = args(2).toInt
        }

        val sampleRate:Double = 1.0

        val conf = new SparkConf
        conf.setAppName("Spark Graphx-CommonFriend Application")
        conf.registerKryoClasses(Array(classOf[Buffer]))
        val sc = new SparkContext(conf)
        val sqlContext: SQLContext = new HiveContext(sc)
        var sl:StorageLevel=StorageLevel.MEMORY_AND_DISK;

        val input = args(0)
        val output = args(1)

        var withoutReplacement:Boolean = false
        val directed:Boolean = true
        val weight:Int = 1

        val inputRdd = sc.textFile(input).filter(_.matches("^[^#].*$")).map{ line =>
            val data = line.split("\\s+")
            val out = new Array[String](2)
            out(0) = data(0)
            out(1) = data(1)
            out
        }

        def pack1(src:Long, dst:Long, weight:Int): (Long, Long, Int) = {
            (src, dst, weight)
        }

        val qualRdd = inputRdd.map{ entry =>
            val src = entry(0).toLong
            val dst = entry(1).toLong
            if ((! directed) && (dst < src)) {
                pack1(dst, src, weight)
            } else {
                pack1(src, dst, weight)
            }
        }.filter(entry => (entry._1 > 0) && (entry._2 > 0))

        val edgeRdd = rddToPairRDDFunctions(qualRdd.sample(withoutReplacement, sampleRate).map{ entry =>
            val src = entry._1
            val dst = entry._2
            val weight = entry._3
            val pid = EdgePartition2D.getPartition(src, dst, partitionNum)
            (pid, (src, dst, weight))
        }).partitionBy(new org.apache.spark.HashPartitioner(partitionNum)).map{ entry =>
            val entry2 = entry._2
            val src = entry2._1
            val dst = entry2._2
            val weight = entry2._3
            val localEdge = new Edge(src, dst, weight)
            localEdge
        }

        val qual = fromEdges(edgeRdd, 0, sl, sl)
        val g = qual.unpersist(true).groupEdges(merge = (e1, e2) => (e1 + e2)).cache()
        val g2: GraphFrame = GraphFrame.fromGraphX(g)

        val msgToSrc = AggregateMessages.dst("id")
        val aggResult = g2.aggregateMessages.sendToSrc(msgToSrc).agg(sort_array(collect_set(AggregateMessages.msg)))
        val g3 = GraphFrame(g2.vertices.join(aggResult, Seq(ID), "outer"), g2.edges)
        g3.triplets.map{ case Row(src:Object, edge:Object, dst:Object) =>
                (1)
        }

        /*
        val nIds = g2.aggregateMessages[Buffer]( triplet => {
            triplet.sendToSrc(Buffer.create(triplet.dstId))
        }, (buf1, buf2) => buf1 ++= buf2)
        val oneNG = g2.outerJoinVertices(nIds) { (vid, vdate, nOpt) =>
            nOpt match {
                case Some(nOpt) => {
                    Predef.longArrayOps(nOpt.toArray(5000)).sortWith((left, right) => left < right)
                }
                case None => {
                    val out = new Array[Long](1)
                    out(0) = 0L
                    out
                }
            }
        }
        oneNG.unpersist(true)
        val cFRdd = oneNG.triplets.map{ edgeTriplet =>
            val srcId = edgeTriplet.srcId
            val dstId = edgeTriplet.dstId
            val srcNeighbors:Array[Long] = edgeTriplet.srcAttr
            val dstNeighbors:Array[Long] = edgeTriplet.dstAttr
            var i:Int = 0
            var j:Int = 0
            var count:Int = 0
            while ((i < srcNeighbors.length) && (j < dstNeighbors.length)) {
                if (srcNeighbors(i) == dstNeighbors(j)) {
                    count += 1
                    i += 1
                    j += 1
                } else if (srcNeighbors(i) > dstNeighbors(j)) {
                    j += 1
                } else {
                    i += 1
                }
            }
            (srcId, dstId, count)
        }

        val resultRdd = cFRdd.map{ case(srcId, dstId, count) =>
            srcId + "\t" + dstId + "\t" + count
        }.saveAsTextFile(output)
        */

        sc.stop()
    }
}
