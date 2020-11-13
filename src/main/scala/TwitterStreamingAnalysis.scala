import UtilityPackage.Utility
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType

import scala.collection.mutable
object TwitterStreamingAnalysis {

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
  def main(args: Array[String]): Unit = {

    val combineWordsUDF = udf(
      combineWords(_: mutable.WrappedArray[String]): String
    )

    val removePunctuationsUDF = udf(removePunctuations(_: String): String)
    val tokenizerUDF = udf(tokenizer(_: String): Array[String])
    val sparkSessionObj =
      Utility.createSessionObject("Twitter Sentiment Analysis")

    sparkSessionObj.sparkContext.setLogLevel("ERROR")
//    val dataFrame = sparkSessionObj.read.textFile(
//      "hdfs://127.0.0.1:9000/user/niraj/tweetCollector/"
//    )
//    dataFrame.show(false)
    val inputDataFrame = sparkSessionObj.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "newTopic")
      .option("startingOffsets", "earliest")
      .load()

    val query = inputDataFrame
      .select("value")
      .withColumn("Reviews", col("value").cast(StringType))
      .writeStream
      .format("console")
      .queryName("Real Time Stock Prediction Query")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    query.awaitTermination(300000)

  }
}
