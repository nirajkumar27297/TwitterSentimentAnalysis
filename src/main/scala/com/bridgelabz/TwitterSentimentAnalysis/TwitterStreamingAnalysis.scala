package com.bridgelabz.TwitterSentimentAnalysis

import java.text.Normalizer

import UtilityPackage.Utility
import com.bridgelabz.PythonHandler.PythonHandler
import org.apache.log4j.Logger
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object TwitterStreamingAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkSessionObj =
      Utility.createSessionObject("Twitter Sentimental Analysis")
    val pythonHandlerObj = new PythonHandler(sparkSessionObj)
    val tweetsStreamingObj =
      new TwitterStreamingAnalysis(sparkSessionObj, pythonHandlerObj)
    val tweetsDataFrame =
      tweetsStreamingObj.takingInputFromKafka("localhost:9092", "newTopic")
    val cleanedTweetsDataFrame =
      tweetsStreamingObj.preProcessingTweets(tweetsDataFrame)
    tweetsStreamingObj.writeToOutputStream(
      cleanedTweetsDataFrame,
      "./PythonFiles/SentimentAnalysis.py"
    )
  }
}
class TwitterStreamingAnalysis(
    sparkSessionObj: SparkSession,
    pythonHandlerObj: PythonHandler
) extends Serializable {
  //Configuring log4j
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def extractingTextPart(review: String): String = {

    val reviews = Normalizer.normalize(review, Normalizer.Form.NFD)
    reviews.toLowerCase
      .replaceAll("[^\\p{ASCII}]", " ")
      .replaceAll("@\\w+ *", " ") //removing words starting with @
      .replaceAll(
        "https?://\\S+",
        " "
      ) //removing words starting with http ot https
      .replaceAll(
        "[^a-z]",
        " "
      )
      .replaceAll(
        """\b\w{1,2}\b""",
        " "
      ) // removing words starting which are not alphabets
      .toLowerCase
      .replaceAll("\\s{2,}", " ")
      .trim

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

  val extractingTextPartUDF = udf(extractingTextPart(_: String): String)
  val tokenizerUDF = udf(tokenizer(_: String): Array[String])

  sparkSessionObj.sparkContext.setLogLevel("ERROR")

  def takingInputFromKafka(brokers: String, topicName: String): DataFrame = {
    val inputDataFrame = sparkSessionObj.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .load()
    inputDataFrame
  }

  private def cleansingTweets(inputDataFrame: DataFrame): DataFrame = {
    try {
      logger.info("Cleansing operation on Tweets")
      val tokenizedTweetDataFrame = inputDataFrame
        .select("value")
        .withColumn("Tweets", col("value").cast(StringType))
        .withColumn(
          "TweetsCleaned",
          extractingTextPartUDF(col("Tweets"))
        )
        .withColumn("TweetsTokenized", tokenizerUDF(col("TweetsCleaned")))
      tokenizedTweetDataFrame
    } catch {
      case ex: org.apache.spark.sql.AnalysisException =>
        logger.info("Difficulty in cleansing operation Exception is" + ex)
        throw new Exception("Spark Sql Analysis Exception")
      case ex: Exception =>
        ex.printStackTrace()
        logger.info("Difficulty in cleansing operation Exception is" + ex)
        throw new Exception("Unexpected Error Occurred")
    }
  }

  def preProcessingTweets(inputDataFrame: DataFrame): DataFrame = {
    //Removing special characters and numbers from Tweets and tokenizing it
    logger.info("Performing PreProcessing Operations on tweets")
    val tokenizedTweetDataFrame = cleansingTweets(inputDataFrame)
    val cleanedTweetsDataFrame = removeStopWords(tokenizedTweetDataFrame)
    cleanedTweetsDataFrame
  }

  private def removeStopWords(inputDataFrame: DataFrame): DataFrame = {
    val remover = new StopWordsRemover()
      .setInputCol("TweetsTokenized")
      .setOutputCol("StopWordsRemovedTweets")

    val stopWordsRemoved = remover.transform(inputDataFrame)

    val cleanedTweetsDataFrame =
      stopWordsRemoved
        .withColumn(
          "CleanedTweets",
          combineWordsUDF(col("StopWordsRemovedTweets"))
        )
        .select("CleanedTweets", "Tweets")
    cleanedTweetsDataFrame
  }

  def predictingSentiment(inputDataFrame: DataFrame, filepath: String): Unit = {
    val polarityScoreOfReviewsDataFrame =
      pythonHandlerObj.performingSentimentAnalysis(
        inputDataFrame,
        filepath
      )
    polarityScoreOfReviewsDataFrame.show(false)
  }

  def writeToOutputStream(inputDataFrame: DataFrame, filepath: String): Unit = {

    val query = inputDataFrame.writeStream
      .foreachBatch { (batchDataFrame: DataFrame, _: Long) =>
        predictingSentiment(batchDataFrame, filepath)
      }
      .queryName("Real Time Stock Prediction Query")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    query.awaitTermination(300000)
  }

}
