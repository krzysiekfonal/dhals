/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.recommendation

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{FloatType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.{Map, SortedMap}
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.hashing.byteswap64

 /**
  * Common params for HALS and HALSModel.
  */
private[recommendation] trait HALSModelParams extends Params with HasPredictionCol {
  /**
   *  Param for the column name for user ids. Ids must be integers. Other
   * numeric types are supported for this column, but will be cast to integers as long as they
   * fall within the integer value range.
   * Default: "user"
   * @group param
   */
  val userCol = new Param[String](this, "userCol", "column name for user ids. Ids must be within " +
    "the integer value range.")

  /** @group getParam */
  def getUserCol: String = $(userCol)

  /**
   * Param for the column name for item ids. Ids must be integers. Other
   * numeric types are supported for this column, but will be cast to integers as long as they
   * fall within the integer value range.
   * Default: "item"
   * @group param
   */
  val itemCol = new Param[String](this, "itemCol", "column name for item ids. Ids must be within " +
    "the integer value range.")

  /** @group getParam */
  def getItemCol: String = $(itemCol)

  /**
   * Attempts to safely cast a user/item id to an Int. Throws an exception if the value is
   * out of integer range or contains a fractional part.
   */
  protected[recommendation] val checkedCast = udf { (n: Any) =>
    n match {
      case v: Int => v // Avoid unnecessary casting
      case v: Number =>
        val intV = v.intValue
        // Checks if number within Int range and has no fractional part.
        if (v.doubleValue == intV) {
          intV
        } else {
          throw new IllegalArgumentException(s"ALS only supports values in Integer range " +
            s"and without fractional part for columns ${$(userCol)} and ${$(itemCol)}. " +
            s"Value $n was either out of Integer range or contained a fractional part that " +
            s"could not be converted.")
        }
      case _ => throw new IllegalArgumentException(s"ALS only supports values in Integer range " +
        s"for columns ${$(userCol)} and ${$(itemCol)}. Value $n was not numeric.")
    }
  }
}

 /**
  * Common params for HALS.
  */
private[recommendation] trait HALSParams extends HALSModelParams with HasMaxIter with HasRegParam
  with HasTol with HasPredictionCol with HasSeed {
  /**
   * Param for rank of the matrix factorization (positive).
   * Default: 10
   * @group param
   */
  val rank = new IntParam(this, "rank", "rank of the factorization", ParamValidators.gtEq(1))

  /** @group getParam */
  def getRank: Int = $(rank)

  /**
   * Param for number of partition the ratings matrix will be split (positive).
   * Default: 10
   * @group param
   */
  val partitions = new IntParam(this, "partitions", "number of parts the ratings matrix is split",
    ParamValidators.gtEq(1))

  /** @group getParam */
  def getPartitions: Int = $(partitions)

  /**
   * Param for rank of the matrix factorization (positive).
   * Default: 10
   * @group param
   */
  val maxInternIter = new IntParam(this, "maxInternIter", "Maximum internal iterations number",
    ParamValidators.gtEq(1))

  /** @group getParam */
  def getMaxInternIter: Int = $(maxInternIter)

  /**
   * Param for the column name for ratings. Ratings column should contains non-negative values.
   * Default: "rating"
   * @group param
   */
  val ratingCol = new Param[String](this, "ratingCol", "column name for ratings")

  /** @group getParam */
  def getRatingCol: String = $(ratingCol)

  /**
   * Param for StorageLevel for intermediate datasets. Pass in a string representation of
   * `StorageLevel`. Cannot be "NONE".
   * Default: "MEMORY_AND_DISK".
   *
   * @group expertParam
   */
  val intermediateStorageLevel = new Param[String](this, "intermediateStorageLevel",
    "StorageLevel for intermediate datasets. Cannot be 'NONE'.",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess && s != "NONE")

  /** @group expertGetParam */
  def getIntermediateStorageLevel: String = $(intermediateStorageLevel)

  /**
   *  Param for StorageLevel for ALS model factors. Pass in a string representation of
   * `StorageLevel`.
   * Default: "MEMORY_AND_DISK".
   *
   * @group expertParam
   */
  val finalStorageLevel = new Param[String](this, "finalStorageLevel",
    "StorageLevel for ALS model factors.",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess)

  /** @group expertGetParam */
  def getFinalStorageLevel: String = $(finalStorageLevel)

  setDefault(rank -> 10, partitions -> 10, maxIter -> 10, maxInternIter -> 10, regParam -> 0.1,
    tol -> 0.1, userCol -> "user", itemCol -> "item", ratingCol -> "rating",
    intermediateStorageLevel -> "MEMORY_AND_DISK", finalStorageLevel -> "MEMORY_AND_DISK")

  /**
   * Validates and transforms the input schema.
   *
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    // user and item will be cast to Int
    SchemaUtils.checkNumericType(schema, $(userCol))
    SchemaUtils.checkNumericType(schema, $(itemCol))
    // rating will be cast to Float
    SchemaUtils.checkNumericType(schema, $(ratingCol))
    SchemaUtils.appendColumn(schema, $(predictionCol), FloatType)
  }
}

@Since("2.4.0")
class HALS(@Since("1.4.0") override val uid: String) extends HALSParams with DefaultParamsWritable {

  import org.apache.spark.ml.recommendation.ALS.Rating

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("hals"))

  /** @group setParam */
  @Since("1.3.0")
  def setRank(value: Int): this.type = set(rank, value)

  /** @group setParam */
  @Since("1.3.0")
  def setPartitions(value: Int): this.type = set(partitions, value)

  /** @group setParam */
  @Since("1.3.0")
  def setTol(value: Double): this.type = set(tol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.3.0")
  def setMaxInternIter(value: Int): this.type = set(maxInternIter, value)

  /** @group setParam */
  @Since("1.3.0")
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  @Since("1.3.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group expertSetParam */
  @Since("2.0.0")
  def setIntermediateStorageLevel(value: String): this.type = set(intermediateStorageLevel, value)

  /** @group expertSetParam */
  @Since("2.0.0")
  def setFinalStorageLevel(value: String): this.type = set(finalStorageLevel, value)

  @Since("2.0.0")
  def fit(dataset: Dataset[_]): Unit = {
    // transformSchema(dataset.schema)
    import dataset.sparkSession.implicits._

    val r = if ($(ratingCol) != "") col($(ratingCol)).cast(FloatType) else lit(1.0f)
    val ratings = dataset
      .select(checkedCast(col($(userCol))), checkedCast(col($(itemCol))), r)
      .rdd
      .map { row =>
        Rating(row.getInt(0), row.getInt(1), row.getFloat(2))
      }

    /* val instr = Instrumentation.create(this, ratings)
    instr.logParams(rank, tol, userCol, itemCol, ratingCol, predictionCol,
      maxIter, stepSize, regParam, seed, intermediateStorageLevel, finalStorageLevel)
    */
    val (userFactors, itemFactors) = HALS.train(ratings, rank = $(rank), partitions = $(partitions),
      maxIter = $(maxIter), maxInternIter = $(maxInternIter), regParam = $(regParam), tol = $(tol),
      intermediateRDDStorageLevel = StorageLevel.fromString($(intermediateStorageLevel)),
      finalRDDStorageLevel = StorageLevel.fromString($(finalStorageLevel)), seed = $(seed))
    val userDF = ratings.sparkContext.makeRDD(userFactors.toSeq).toDF("id", "features")
    val itemDF = ratings.sparkContext.makeRDD(itemFactors.toSeq).toDF("id", "features")
    /*
    val model = new ALSModel(uid, $(rank), userDF, itemDF).setParent(this)
    instr.logSuccess(model)
    copyValues(model)
    */
  }

  /*
  @Since("1.3.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
  */

  @Since("1.5.0")
  override def copy(extra: ParamMap): ALS = defaultCopy(extra)
}

 /**
  * :: DeveloperApi ::
  * An implementation of ALS that supports generic ID types, specialized for Int and Long. This is
  * exposed as a developer API for users who do need other ID types. But it is not recommended
  * because it increases the shuffle size and memory requirement during training. For simplicity,
  * users and items must have the same type. The number of distinct users/items should be smaller
  * than 2 billion.
  */
@DeveloperApi
object HALS extends DefaultParamsReadable[HALS] with Logging {

  private[recommendation] class Matrix(val n: Int, val m: Int, val initValue: Float)
    extends Serializable{
    private val values = Array.fill[Float](n * m)(initValue)

    def this(n: Int, m: Int) {
      this(n, m, 0.0f)
    }

    def apply(i: Int, j: Int): Float = values(i + j*n)

    def update(i: Int, j: Int, v: Float): Unit = values(i + j*n) = v

    def updateAdd(i: Int, j: Int, v: Float): Unit = values(i + j*n) += v

    def add(other: Matrix): this.type = {
      for (i <- 0 until values.length) {
        values(i) += other.values(i);
      }
      this
    }

    def scalarRowMultiply(row: Int, vec: Array[Float]): Float = {
      var value = 0.0f
      for (j <- 0 until m) {
        value += apply(row, j) * vec(j)
      }
      value
    }
  }

  @DeveloperApi
  def train[ID: ClassTag]( // scalastyle:ignore
                           ratings: RDD[Rating[ID]],
                           rank: Int = 10,
                           partitions: Int = 10,
                           maxIter: Int = 10,
                           maxInternIter: Int = 10,
                           regParam: Double = 0.1,
                           tol: Double = 1e-12,
                           intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
                           finalRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
                           seed: Long = 0L)(
                           implicit ord: Ordering[ID]):
  (Map[ID, Array[Float]], Map[ID, Array[Float]]) = {

    require(!ratings.isEmpty(), s"No ratings available from $ratings")
    require(intermediateRDDStorageLevel != StorageLevel.NONE,
      "ALS is not designed to run without persisting intermediate RDDs.")

    val sc = ratings.sparkContext

    val (colRaitings, rowRaitings) =
      partitionRatings(ratings, partitions, intermediateRDDStorageLevel)
    System.out.println("Data partitioned\n")
    val seedGen = new XORShiftRandom(seed)
    val (userFactors, itemFactors) =
      initializeFactors(colRaitings, rowRaitings, rank, seedGen.nextLong())
    System.out.println("Factors initialized\n")

    var tolU, tolI = 0.0
    for (k <- 1 to maxIter) {
      System.out.println("Iter " + k + "\n")
      computeFactors(colRaitings, itemFactors, userFactors, rank, maxInternIter, tolI)
      normalizeFactors(itemFactors, userFactors, rank)
      computeFactors(rowRaitings, userFactors, itemFactors, rank, maxInternIter, tolU)
      normalizeFactors(userFactors, itemFactors, rank)
    }

    if (finalRDDStorageLevel != StorageLevel.NONE) {
      colRaitings.unpersist()
      rowRaitings.unpersist()
    }

    (userFactors, itemFactors)
  }

  private def partitionRatings[ID: ClassTag](ratings: RDD[Rating[ID]],
                                             partitions: Int,
                                             intermediateRDDStorageLevel: StorageLevel)
  : (RDD[(ID, Iterable[(ID, Float)])], RDD[((ID, Iterable[(ID, Float)]))]) = {
    val colRatings = ratings.
      mapPartitions(iter => iter.map( u => ( u.item, (u.user, u.rating)) ) ).
      groupByKey(/*new HashPartitioner(partitions)*/).
      persist(intermediateRDDStorageLevel)

    val rowRatings = ratings.
      mapPartitions(iter => iter.map( u => ( u.user, (u.item, u.rating)) ) ).
      groupByKey(/*new HashPartitioner(partitions)*/).
      persist(intermediateRDDStorageLevel)

    (colRatings, rowRatings)
  }

  private def initializeFactors[ID: ClassTag](colRatings: RDD[(ID, Iterable[(ID, Float)])],
                                              rowRatings: RDD[(ID, Iterable[(ID, Float)])],
                                              rank: Int,
                                              seed: Long = 0L)(implicit ord: Ordering[ID])
  : (Map[ID, Array[Float]], Map[ID, Array[Float]]) = {
    val rowFactors = rowRatings.map(q => {
      val (k, _) = q
      val random = new XORShiftRandom(byteswap64(seed ^ k.hashCode() + rowRatings.hashCode()))
      val factor = Array.fill(rank)(math.max(0.0f, random.nextGaussian().toFloat))
      //val nrm = blas.snrm2(rank, factor, 1)
      //blas.sscal(rank, 1.0f / nrm, factor, 1)
      (k, factor)
    }).collect()
    System.out.println("Calculated factors: " + rowFactors.length)
    val colFactors = colRatings.map(q => {
      val (k, _) = q
      val random = new XORShiftRandom(byteswap64(seed ^ k.hashCode() + rowRatings.hashCode()))
      val factor = Array.fill(rank)(math.max(0.0f, random.nextGaussian().toFloat))
      //val nrm = blas.snrm2(rank, factor, 1)
      //blas.sscal(rank, 1.0f / nrm, factor, 1)
      (k, factor)
    }).collect()
    System.out.println("Calculated factors: " + colFactors.length)

    (SortedMap(rowFactors.toSeq: _*),SortedMap(colFactors.toSeq: _*))
  }

  private def computeB[ID: ClassTag](factors: Map[ID, Array[Float]], rank: Int): Matrix = {
    factors.par.aggregate(new Matrix(rank, rank, Float.MinPositiveValue))(
      (acc, f) => {
        val(_, v) = f
        for (i <- 0 until rank)
          for (j <- 0 until rank)
            acc.updateAdd(i, j, v(i) * v(j))
        acc
      },
      (acc1, acc2) => (acc1.add(acc2))
    )
  }

  private def computeC[ID: ClassTag](ratings: RDD[(ID, Iterable[(ID, Float)])],
                                     factors: Map[ID, Array[Float]],
                                     rank: Int): Map[ID, Matrix] = {
    ratings.aggregateByKey(new Matrix(rank, 1))(
      (acc, vec) => {
        for ((id, value) <- vec) {
          for (i <- 0 until rank) {
            acc.updateAdd(i, 0, value * factors(id)(i))
          }
        }
        acc
      },
      (acc1, acc2) => acc1.add(acc2)
    ).collectAsMap()
  }

  private def computeNormGrad[ID: ClassTag](factors: Map[ID, Array[Float]],
                                         b: Matrix,
                                         c: Map[ID, Matrix],
                                         rank: Int): Float = {
    math.sqrt(
      factors.par.aggregate(0.0f)((acc, it) => {
        val(k, v) = it
        val cV = c(k)
        var _acc = acc
        for (i <- 0 until rank) {
          val gV = b.scalarRowMultiply(i, v) - cV(i, 0) // A*B-C
          if (gV < 0 || v(i) > 0) _acc += (gV * gV) // norm(G(G < 0 | A > 0)
        }
        _acc
      }, (acc1, acc2) => acc1 + acc2)
    ).toFloat
  }

  private def normalizeFactors[ID: ClassTag](normalizingFactors: Map[ID, Array[Float]],
                                             counterFactors: Map[ID, Array[Float]],
                                             rank: Int): Unit =
  {
    val (_, sumVector: Array[Float]) = normalizingFactors.par.reduce((f1, f2) =>
      (0.asInstanceOf[ID], (f1._2, f2._2).zipped.map(_ + _ + Float.MinPositiveValue)))
    // normalize one factors map
    normalizingFactors.par.foreach(it => {
      val (_, v) = it
      for (i <- 0 until rank) v(i) = v(i) / sumVector(i)
    })

    // opposite operation for second factors map
    counterFactors.par.foreach(it => {
      val (_, v) = it
      for (i <- 0 until rank) v(i) = v(i) * sumVector(i)
    })
  }

  private def computeFactors[ID: ClassTag](ratings: RDD[(ID, Iterable[(ID, Float)])],
                                           mutableFactors: Map[ID, Array[Float]],
                                           constFactors: Map[ID, Array[Float]],
                                           rank: Int,
                                           maxInternIter: Int,
                                           tol: Double
                                          ): Unit = {
    val broadcast = ratings.sparkContext.broadcast(constFactors)
    val b = computeB(constFactors, rank)
    val c = computeC(ratings, constFactors, rank)
    broadcast.unpersist()
    broadcast.destroy()

    for (k <- 1 to maxInternIter) {
      for (j <- 0 until rank) {
        mutableFactors.par.foreach(it => {
          val(k, v) = it
          v(j) = Math.max(0.0f, v(j) + (c(k)(j, 0) - b.scalarRowMultiply(j, v)) / b(j, j))
        })
      }
    }
  }

}

