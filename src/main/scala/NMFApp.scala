import java.io.{FileWriter}

import org.apache.spark.sql.SparkSession

object NMFApp {
  def main(args: Array[String]): Unit ={
    if (args.length < 9) {
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
    val logFilename = args(8)

    val session = SparkSession.builder().appName("Distributed Fast Hals").getOrCreate()

    for (i <- minIter to maxIter by stepBy) {
      if (algorithm.equals("ALS")) {
        session.sparkContext.setCheckpointDir("/tmp/spark-cp")
        printResults(logFilename,
          TestLauncher.launchALSTest(session, dataFilename, rank, partitions, i, mc) )
      } else if (algorithm.equals("HALS")) {
        printResults(logFilename,
          TestLauncher.launchHALSTest(session, dataFilename, rank, partitions, i, 10, mc) )
      }
    }

    session.stop()
  }

  def printResults(logFilename: String, results: List[String]): Unit = {
    val pw = new FileWriter(logFilename, true)
    pw.write(results.reduce((s1, s2)=>s1 + "\n" + s2) + "\n")
    pw.close()
  }

  def printUsages() = {
    System.out.println("NMFApp.jar file-with-data algorithm(ALS|HALS) rank partitions minIter maxIter stepBy mc log-filename")
  }
}
