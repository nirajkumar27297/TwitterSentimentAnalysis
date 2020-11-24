/**
  * The objective is to get the data from ka cluster ,push it through spark streaming in batches and predict
  * the sentiment score  price for tweets and save it as a csv file.
  * Library Used -
  * 1> org.apache.spark.spark-sql
  *  Version - 3.0.0
  * 2> org.apache.spark.spark-core
  *   Version - 3.0.0
  * 3> org.apache.spark.spark-Streaming
  *  Version - 3.0.0
  * 4> org.apache.spark.spark-mllib
  *    Version - 3.0.0
  *
  *    @author:Niraj
  *    *
  */
package com.bridgelabz.TwitterSentimentAnalysisPackage

import UtilityPackage.Utility
import com.bridgelabz.AWSUtilites.{S3Configurations, S3Upload}
import com.bridgelabz.PythonHandlerPackage.PythonHandler
import org.apache.log4j.Logger
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions.{col, current_timestamp, udf}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  *
  */
object TwitterStreamingAnalysis extends App {
  val sparkSessionObj =
    Utility.createSessionObject("Twitter Sentimental Analysis")
  S3Configurations.connectToS3(sparkSessionObj.sparkContext)
  val pythonHandlerObj = new PythonHandler(sparkSessionObj)
  val tweetsStreamingObj =
    new TwitterStreamingAnalysis(sparkSessionObj, pythonHandlerObj)
  val tweetsDataFrame =
    tweetsStreamingObj.takingInputFromKafka(args(0), args(1))
  val cleanedTweetsDataFrame =
    tweetsStreamingObj.preProcessingTweets(tweetsDataFrame)
  tweetsStreamingObj.writeToOutputStream(
    cleanedTweetsDataFrame,
    args(2),
    args(3)
  )
}

/**
  *
  * @param sparkSessionObj
  * @param pythonHandlerObj
  */
class TwitterStreamingAnalysis(
    sparkSessionObj: SparkSession,
    pythonHandlerObj: PythonHandler
) extends Serializable {
  //Configuring log4j
  lazy val logger: Logger = Logger.getLogger(getClass.getName)
  // Registering combineWords User Defined Function
  val combineWordsUDF = udf(
    CleansingOperations.combineWords(_: mutable.WrappedArray[String]): String
  )

  // Registering extractingTextPart User Defined Function
  val extractingTextPartUDF = udf(
    CleansingOperations.extractingTextPart(_: String): String
  )
  // Registering tokenizer User Defined Function
  val tokenizerUDF = udf(
    CleansingOperations.tokenizer(_: String): Array[String]
  )

  /**
    * This function is used to take input from kafka topic taking brokers and topicName as arguments.
    * @param brokers [String]
    * @param topicName [String]
    * @return DataFrame
    */

  def takingInputFromKafka(brokers: String, topicName: String): DataFrame = {
    try {
      logger.info("Taking input from kafka Topic")
      val inputDataFrame = sparkSessionObj.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topicName)
        .option("startingOffsets", "earliest")
        .load()
      inputDataFrame
    } catch {
      case ex: org.apache.kafka.common.KafkaException => {
        ex.printStackTrace()
        logger.info("Difficulty in taking input from kafka brokers" + ex)
        throw new Exception("Difficulty in taking input from kafka brokers")
      }
      case ex: org.apache.kafka.common.config.ConfigException => {
        logger.info(
          "Difficulty in starting the Streaming Services and Exception is" + ex
        )
        throw new Exception("Spark Sql Analysis Exception")
      }

      case ex: org.apache.spark.sql.AnalysisException => {
        ex.printStackTrace()
        logger.info("Difficulty in taking input from kafka brokers" + ex)
        throw new Exception("Difficulty in taking input from kafka brokers")
      }
      case ex: Exception => {
        ex.printStackTrace()
        logger.info("Difficulty in cleansing operation Exception is" + ex)
        throw new Exception(
          "Unexpected Error Occurred while Taking Input from kafka brokers"
        )
      }
    }
  }

  /**
    * The objective of the function is to perform cleaning operation on Tweets.
    * Like extracting only the text part and tokenizing the sentences.
    * @param inputDataFrame [DataFrame]
    * @return [DataFrame]
    */
  private def cleansingTweets(inputDataFrame: DataFrame): DataFrame = {
    try {
      logger.info("Cleansing operation on Tweets")
      val tokenizedTweetDataFrame = inputDataFrame
        .select("value", "key")
        .withColumn("Time", col("key").cast(StringType))
        .withColumn("Tweets", col("value").cast(StringType))
        .withColumn(
          "TweetsCleaned",
          extractingTextPartUDF(col("Tweets"))
        )
        .withColumn("TweetsTokenized", tokenizerUDF(col("TweetsCleaned")))
      tokenizedTweetDataFrame
    } catch {
      case ex: org.apache.spark.sql.AnalysisException => {
        logger.info("Difficulty in cleansing operation Exception is" + ex)
        throw new Exception("Spark Sql Analysis Exception")
      }
      case ex: Exception => {
        ex.printStackTrace()
        logger.info("Difficulty in cleansing operation Exception is" + ex)
        throw new Exception("Unexpected Error Occurred")
      }
    }
  }

  /**
    * The objective of the function is to perform PreProcessing on Tweets.
    * It call two function cleansingTweets and removeStopWords
    * @param inputDataFrame [DataFrame]
    * @return [DataFrame]
    */
  def preProcessingTweets(inputDataFrame: DataFrame): DataFrame = {
    //Removing special characters and numbers from Tweets and tokenizing it
    logger.info("Performing PreProcessing Operations on tweets")
    val tokenizedTweetDataFrame = cleansingTweets(inputDataFrame)
    val cleanedTweetsDataFrame = removeStopWords(tokenizedTweetDataFrame)
    cleanedTweetsDataFrame
  }

  /**
    * The objective of the function is to remove stop words from tweets.
    * @param inputDataFrame [DataFrame]
    * @return [DataFrame]
    */
  private def removeStopWords(inputDataFrame: DataFrame): DataFrame = {
    try {
      logger.info("Removing Stop Words From Tweets")
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
          .withColumn("Time", current_timestamp())
          .select("CleanedTweets", "Tweets", "Time")
      cleanedTweetsDataFrame
    } catch {
      case ex: org.apache.spark.sql.AnalysisException => {
        logger.info("Difficulty in cleansing operation Exception is" + ex)
        throw new Exception("Spark Sql Analysis Exception")
      }
      case ex: Exception => {
        ex.printStackTrace()
        logger.info("Difficulty in cleansing operation Exception is" + ex)
        throw new Exception("Unexpected Error Occurred")
      }
    }
  }

  /**
    * The function is to perform sentimental analysis on tweets using PythonHandler class Functions.
    * @param inputDataFrame
    * @param filepath
    * @param pathToSave
    */
  def getSentimentScore(
      inputDataFrame: DataFrame,
      filepath: String,
      pathToSave: String
  ): Unit = {
    logger.info("Performing Sentimental Analysis on Tweets")

    if (!inputDataFrame.isEmpty) {
      val polarityScoreOfReviewsDataFrame =
        pythonHandlerObj.performingSentimentAnalysis(
          inputDataFrame,
          filepath
        )
      //Saving the output dataframe as csv in the provided path
      if (pathToSave.startsWith("s3a://")) {
        val startPosition = pathToSave.indexOf("//") + 2
        val lastPosition = pathToSave.lastIndexOf("/")
        val bucketName = pathToSave.substring(startPosition, lastPosition)
        println(bucketName)
        if (!S3Upload.checkBucketExistsOrNot(bucketName)) {
          S3Upload.createBucket(bucketName)
        }
      }

      polarityScoreOfReviewsDataFrame.show(5)
      polarityScoreOfReviewsDataFrame.write
        .mode("append")
        .option("header", value = true)
        .csv(pathToSave)
    }
  }

  /**
    * The function is for starting and stopping the streaming services along with calling
    * @param inputDataFrame
    * @param filepath
    * @param pathToSave
    */
  def writeToOutputStream(
      inputDataFrame: DataFrame,
      filepath: String,
      pathToSave: String
  ): Unit = {
    try {
      logger.info("Starting the streaming services")
      val query = inputDataFrame.writeStream
        .foreachBatch { (batchDataFrame: DataFrame, _: Long) =>
          getSentimentScore(batchDataFrame, filepath, pathToSave)
        }
        .queryName("Real Time Stock Prediction Query")
        .option("checkpointLocation", "chk-point-dir-twitter")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()
      logger.info("Terminating the streaming services")
      query.awaitTermination()
    } catch {
      case ex: org.apache.spark.sql.AnalysisException => {
        logger.info(
          "Difficulty in starting the Streaming Services and Exception is" + ex
        )
        throw new Exception("Spark Sql Analysis Exception")
      }
      case ex: Exception => {
        ex.printStackTrace()
        logger.info(
          "Difficulty in starting the Streaming Services and Exception is" + ex
        )
        throw new Exception("Unexpected Error Occurred")
      }
    }
  }

}
