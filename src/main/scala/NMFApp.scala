import java.io.FileWriter

import org.apache.spark.ml.recommendation.{ALS, HALS}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{FloatType, StructField, StructType}

object NMFApp {
  def main(args: Array[String]): Unit ={
    if (args.length == 4) {
      val srcFilename = args(0)
      val dstFilename = args(1)
      val partitions = args(2).toInt
      val header = args(3).toInt > 0
      val aCol: String = "userId"
      val xCol: String = "movieId"
      val vCol: String = "rating"
      val session = SparkSession.builder().appName("Distributed Fast Hals").getOrCreate()
      val schema = StructType(Array(StructField(aCol, FloatType, true),
        StructField(xCol, FloatType, true),
        StructField(vCol, FloatType, true)))
      val data = session.read.option("header", header).schema(schema).csv(srcFilename)
      val ratings = data.select(col(aCol), col(xCol), col(vCol)).rdd.map { row =>
        ALS.Rating(row.getFloat(0).toInt, row.getFloat(1).toInt, row.getFloat(2))
      }
      println("Read srcFile")
      ratings.repartition(partitions).saveAsObjectFile(dstFilename)
      println("Save dstFile")
      return
    }else if (args.length < 8) {
      printUsages()
      return
    }

    val dataFilename = args(0)
    val algorithm = args(1)
    val rank = args(2).toInt
    val partitions = args(3).toInt
    val minIter = args(4).toInt
    val maxIter = args(5).toInt
    val stepBy = args(6).toInt
    val mc = args(7).toInt
    val header = if (args.length >= 9) args(8).toInt > 0 else false
    val logFilename = if (args.length == 10) args(9) else null

    val session = SparkSession.builder().appName("Distributed Fast Hals").getOrCreate()

    val iterations = if (minIter == -1) List[Int](1,2,4,8,16) else List.range(minIter, maxIter, stepBy)
    for (i <- iterations) {
      if (algorithm.equals("ALS")) {
        session.sparkContext.setCheckpointDir("/tmp/spark-cp")
        printResults(logFilename,
          TestLauncher.launchALSTest(session, dataFilename, rank, partitions, i, mc, header) )
      } else if (algorithm.equals("HALS")) {
        printResults(logFilename,
          TestLauncher.launchHALSTest(session, dataFilename, rank, partitions, i, 5, mc, header) )
      }
    }

    session.stop()
  }

  def printResults(logFilename: String, results: List[String]): Unit = {
    if (logFilename != null) {
      val pw = new FileWriter(logFilename, true)
      pw.write(results.reduce((s1, s2) => s1 + "\n" + s2) + "\n")
      pw.close()
    } else {
      System.out.println(results.reduce((s1, s2) => s1 + "\n" + s2) + "\n")
    }
  }

  def printUsages() = {
    System.out.println("NMFApp.jar file-with-data algorithm(ALS|HALS) rank partitions minIter maxIter stepBy mc [log-filename]")
  }
}
