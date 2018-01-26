import java.util.Random

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.recommendation.{ALS, HALS}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{FloatType, StructField, StructType}

import scala.collection.Map

object TestLauncher {
  def launchHALSTest(session: SparkSession,
                 filename: String,
                 rank: Int,
                 partitions: Int,
                 maxIter: Int,
                 maxInterIter: Int,
                 mc: Int,
                 header: Boolean = true,
                 aCol: String = "userId",
                 xCol: String = "movieId",
                 vCol: String = "rating"
                ): List[String] = {
    var resultsLog = List.empty[String]
    val schema = StructType(Array(StructField(aCol, FloatType, true),
      StructField(xCol, FloatType, true),
      StructField(vCol, FloatType, true)))
    val data = session.read.option("header", header).schema(schema).csv(filename)
    val ratings: RDD[ALS.Rating[Int]] =
      if (filename.endsWith(".obj")) session.sparkContext.objectFile(filename) else
      data.select(col(aCol), col(xCol), col(vCol)).rdd.map { row =>
        ALS.Rating(row.getFloat(0).toInt, row.getFloat(1).toInt, row.getFloat(2))
      }
    val ratingNorm = calcNorm(ratings)
    System.out.println(ratings.count())

    for (i <- 1 to mc) {
      var currentTime = System.currentTimeMillis()

      val (userFactors, itemFactors) = HALS.train(ratings, rank, partitions, maxIter, maxInterIter, seed = new Random().nextLong())
      val time = System.currentTimeMillis() - currentTime

      val diffNorm = calcDiffNorm(ratings, userFactors, itemFactors)
      val res = diffNorm / ratingNorm
      resultsLog = resultsLog:+(i + "," + maxIter + "," + time + "," + res)
    }

    resultsLog
  }

  def launchALSTest(session: SparkSession,
                 filename: String,
                 rank: Int,
                 partitions: Int,
                 maxIter: Int,
                 mc: Int,
                 header: Boolean = true,
                 aCol: String = "userId",
                 xCol: String = "movieId",
                 vCol: String = "rating"
                ): List[String] = {
    var resultsLog = List.empty[String]
    val schema = StructType(Array(StructField(aCol, FloatType, true),
      StructField(xCol, FloatType, true),
      StructField(vCol, FloatType, true)))
    val data = session.read.option("header", header).schema(schema).csv(filename)
    val ratings = data.select(col(aCol), col(xCol), col(vCol)).rdd.map { row =>
      ALS.Rating(row.getFloat(0).toInt, row.getFloat(1).toInt, row.getFloat(2))
    }

    val ratingNorm = calcNorm(ratings)

    for (i <- 1 to mc) {
      var currentTime = System.currentTimeMillis()

      val (userFactors, itemFactors) = ALS.train(ratings,
        rank,
        partitions,
        partitions,
        maxIter,
        nonnegative = true,
        seed = new Random().nextLong())
      val time = System.currentTimeMillis() - currentTime

      val diffNorm = calcDiffNorm(ratings, userFactors.collectAsMap(), itemFactors.collectAsMap())
      val res = diffNorm / ratingNorm

      resultsLog = resultsLog:+(i + "," + maxIter + "," + time + "," + res)
    }

    resultsLog
  }

  def calcDiffNorm(ratings: RDD[Rating[Int]],
                   userFactors: Map[Int, Array[Float]],
                   itemFactors: Map[Int, Array[Float]]) = {
    val b1 = ratings.sparkContext.broadcast(userFactors)
    val b2 = ratings.sparkContext.broadcast(itemFactors)
    val res = Math.sqrt(
      ratings.aggregate(0.0)(
        (acc, q)=>{
          acc + Math.pow((q.rating - ((userFactors(q.user),itemFactors(q.item)).zipped.map(_ * _).sum)), 2)
        },
        (acc1, acc2)=>acc1 + acc2)
    )
    b1.unpersist()
    b2.unpersist()
    b1.destroy()
    b2.destroy()
    res
  }

  def calcNorm(ratings: RDD[Rating[Int]]) = {
    Math.sqrt(
      ratings.aggregate(0.0)(
        (acc, q)=>{
          acc + Math.pow(q.rating, 2)
        },
        (acc1, acc2)=>acc1 + acc2)
    )
  }
}
