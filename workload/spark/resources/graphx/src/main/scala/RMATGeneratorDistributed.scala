package src.main.scala
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.ListBuffer
import scala.math._
import scala.util.Random

object RMATGeneratorDistributed {

  val RMATa = 0.45
  val RMATb = 0.15
  val RMATc = 0.15
  val RMATd = 0.25

  def pickQuadrant(rand:Random, a: Double, b: Double, c: Double, d: Double): Int = {
    if (a + b + c + d != 1.0) {
      throw new IllegalArgumentException("R-MAT probability parameters sum to " + (a + b + c + d)
        + ", should sum to 1.0")
    }
    val result = rand.nextDouble()
    result match {
      case x if x < a => 0 // 0 corresponds to quadrant a
      case x if (x >= a && x < a + b) => 1 // 1 corresponds to b
      case x if (x >= a + b && x < a + b + c) => 2 // 2 corresponds to c
      case _ => 3 // 3 corresponds to d
    }
  }

  def chooseCell(r:Random, x: Long, y: Long, t: Long): (Long, Long) = {
    if (t <= 1) {
      (x, y)
    } else {
      val newT = math.round(t.toFloat/2.0).toLong
      pickQuadrant(r, RMATa, RMATb, RMATc, RMATd) match {
        case 0 => chooseCell(r, x, y, newT)
        case 1 => chooseCell(r, x + newT, y, newT)
        case 2 => chooseCell(r, x, y + newT, newT)
        case 3 => chooseCell(r, x + newT, y + newT, newT)
      }
    }
  }

  def main(args: Array[String]) {
    /*
        Problem Class   V Scale E/V approx storage (TB)
        toy (level 10)  26  16  0.0172
        mini (level 11) 29  16  0.1374
        small (level 12)    32  16  1.0995
        medium (level 13)   36  16  17.5922
        large (level 14)    39  16  140.7375
        huge (level 15) 42  16  1125.8999
    */
    if (args.length < 4) {
      println("usage: <output> <Vertices scale> <Edge/Vetrics ratio> <numPartitions>")
      System.exit(0)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)   
    val conf = new SparkConf
    conf.setAppName("RMATGeneratorDistributed") 
    val sc = new SparkContext(conf)
        
    val output = args(0)
    val numVertices = math.pow(2.0, args(1).toInt).toLong
    val numEdges = numVertices * args(2).toLong
    val numPar= args(3).toInt
    
    var count = 0
    var tmpNumPar = numPar
    while (tmpNumPar != 1) {
        count = count + 1
        tmpNumPar = tmpNumPar / 4
    }

    def pack_elem(idx:Long, r:Long, c:Long): (Long, (Long, Long)) = {
        (idx, (r, c))
    }

    val resultRdd = sc.parallelize(0 to (numPar-1), numPar).flatMap{ idx =>
        val rand = new Random(idx + 42)
        var edgeList = new ListBuffer[(Long, (Long, Long))]()
        var factors = new ListBuffer[Int]()
        var i = idx
        var c = 0
        while (i >= 4) {
            factors += i % 4;
            c = c + 1
            i = i / 4
        }
        factors += i;
        c = c + 1
        while (c < count) {
            factors += 0
            c = c + 1
        }
        val rList = factors.toList.reverse
        val rListStr = rList mkString "/"
        var edges = numEdges
        var startRow = 0L
        var startCol = 0L
        var vertDiv = numVertices
        for (i <- 0 to (count - 1)) {
            if (rList(i) == 0) {
                edges = (edges * RMATa).toLong
            } else if (rList(i) == 1) {
                startCol = startCol + vertDiv / 2L
                edges = (edges * RMATb).toLong
            } else if (rList(i) == 2) {
                startRow = startRow + vertDiv / 2L
                edges = (edges * RMATc).toLong
            } else {
                startRow = startRow + vertDiv / 2L
                startCol = startCol + vertDiv / 2L
                edges = (edges * RMATd).toLong
            }
            vertDiv = vertDiv / 2L
        }

        var total = 0
        while (total < edges) {
            val (r, c) = chooseCell(rand, vertDiv, vertDiv, vertDiv)
            edgeList += pack_elem((r + startRow + (c + startCol) * numVertices), (r + startRow), (c + startCol))
            total = total + 1
        }
        edgeList
        //(idx, rListStr, edges, startRow, startCol, vertDiv)
    }.distinct().sortByKey().map{ case(idx, (r, c)) => r + " " + c + " 1"}.saveAsTextFile(output)

    //resultRdd.map{ case(idx, (r, c)) => r + " " + c + " 1"}.saveAsTextFile(output)
    //val graph= GraphGenerators.logNormalGraph(sc,numVertices,numPar,mu,sigma)
	
	//graph.edges.map(s=> s.srcId.toString+" "+s.dstId.toString+" "+s.attr.toString).saveAsTextFile(output)
    
    sc.stop();
  }

}
