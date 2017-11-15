import java.util.Random

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.recommendation.{ALS, HALS}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}

import scala.collection.Map

object TestLauncher {
  def launchHALSTest(session: SparkSession,
                 filename: String,
                 rank: Int,
                 partitions: Int,
                 maxIter: Int,
                 maxInterIter: Int,
                 mc: Int,
                 aCol: String = "userId",
                 xCol: String = "movieId",
                 vCol: String = "rating"
                ): List[String] = {
    var resultsLog = List.empty[String]
    val schema = StructType(Array(StructField(aCol, IntegerType, true),
      StructField(xCol, IntegerType, true),
      StructField(vCol, FloatType, true)))
    val data = session.read.option("header", true).schema(schema).csv(filename)
    val ratings = data.select(col(aCol), col(xCol), col(vCol)).rdd.map { row =>
      ALS.Rating(row.getInt(0), row.getInt(1), row.getFloat(2))
    }

    val ratingNorm = calcNorm(ratings)

    for (i <- 1 to mc) {
      var currentTime = System.currentTimeMillis()

      val (userFactors, itemFactors) = HALS.train(ratings, rank, partitions, maxIter, maxInterIter, seed = new Random().nextLong())
      val time = System.currentTimeMillis() - currentTime

      val diffNorm = calcDiffNorm(ratings, userFactors, itemFactors)
      val res = diffNorm / ratingNorm
      println("HALS time: " + time + " Res. error: " + res)
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
                 aCol: String = "userId",
                 xCol: String = "movieId",
                 vCol: String = "rating"
                ): List[String] = {
    var resultsLog = List.empty[String]
    val schema = StructType(Array(StructField(aCol, IntegerType, true),
      StructField(xCol, IntegerType, true),
      StructField(vCol, FloatType, true)))
    val data = session.read.option("header", true).schema(schema).csv(filename)
    val ratings = data.select(col(aCol), col(xCol), col(vCol)).rdd.map { row =>
      ALS.Rating(row.getInt(0), row.getInt(1), row.getFloat(2))
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

      println("ALS time: " + time + " Res. error: " + res)
      resultsLog = resultsLog:+(i + "," + maxIter + "," + time + "," + res)
    }

    resultsLog
  }

  def calcDiffNorm(ratings: RDD[Rating[Int]],
                   userFactors: Map[Int, Array[Float]],
                   itemFactors: Map[Int, Array[Float]]) = {
    Math.sqrt(
      ratings.aggregate(0.0)(
        (acc, q)=>{
          acc + Math.pow((q.rating - ((userFactors(q.user),itemFactors(q.item)).zipped.map(_ * _).sum)), 2)
        },
        (acc1, acc2)=>acc1 + acc2)
    )
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
