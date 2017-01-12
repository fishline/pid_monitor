import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors

object KmeansApp {
  def main(args: Array[String]) {
    if (args.length < 5) {
      println("usage (minimum parameter: 5): <input> <output> <numClusters> <numIterations> <partition> <run> <initMode: k-means|| or random> <epsilon>")
      System.exit(0)
    }

    val conf = new SparkConf
    conf.setAppName("Spark KMeans Example")
    
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val numClusters = args(2).toInt
    val numIterations = args(3).toInt
    val partition = args(4).toInt
    var run = 1
    var initMode = "k-means||"

    if (args.length >= 6) {
        run = args(5).toInt
    }
   
    if (args.length >= 7) {
        initMode = args(6)
    }
   
    // Load and parse the data
    val data = sc.textFile(input, partition)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    if (args.length >= 8) {
        val clusters = new KMeans().setEpsilon(args(7).toDouble)
                    .setK(numClusters)
                    .setMaxIterations(numIterations)
                    .setRuns(run)
                    .setInitializationMode(initMode)
                    .run(parsedData)
    } else {
        val clusters = KMeans.train(parsedData, numClusters, numIterations, run, initMode)
    }

    sc.stop()
  }
}
