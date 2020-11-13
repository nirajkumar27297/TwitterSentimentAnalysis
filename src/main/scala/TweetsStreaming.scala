import UtilityPackage.Utility
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable
object TweetsStreaming extends App {

  def removePunctuations(review: String): String = {
    review
      .replaceAll("""([\p{Punct}]|\b\p{IsLetter}{1,2}\b)""", " ")
      .replaceAll("\\s{2,}", " ")
      .trim
      .toLowerCase
  }

  def tokenizer(review: String): Array[String] = {
    review.split(" ")
  }
  def combineWords(review: mutable.WrappedArray[String]): String = {
    review.mkString(" ")
  }
  val combineWordsUDF = udf(
    combineWords(_: mutable.WrappedArray[String]): String
  )

  val removePunctuationsUDF = udf(removePunctuations(_: String): String)
  val tokenizerUDF = udf(tokenizer(_: String): Array[String])
  val sparkSessionObj =
    Utility.createSessionObject("Twitter Sentiment Analysis")

  sparkSessionObj.sparkContext.setLogLevel("ERROR")
  val dataFrame = sparkSessionObj.read
    .option("delimiter", "\t")
    .csv("./Data/amazonReviews.txt")
    .withColumnRenamed("_c0", "Reviews")
    .withColumnRenamed("_c1", "Polarity")
    .withColumn("Polarity", col("Polarity").cast(IntegerType))

  val newDataFrame = dataFrame
    .withColumn(
      "ReviewsCleaned",
      removePunctuationsUDF(col("Reviews"))
    )
    .withColumn("ReviewsTokenized", tokenizerUDF(col("ReviewsCleaned")))
  val remover = new StopWordsRemover()
    .setInputCol("ReviewsTokenized")
    .setOutputCol("stopWordsRemovedReviews")

  val stopWordsRemoved = remover.transform(newDataFrame)

  stopWordsRemoved.show(5)

  val cleanedReviewsDataFrame =
    stopWordsRemoved.withColumn(
      "CleanedReviews",
      combineWordsUDF(col("stopWordsRemovedReviews"))
    )

  cleanedReviewsDataFrame.show(false)

}
