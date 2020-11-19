import java.io.Serializable
import java.text.Normalizer

import UtilityPackage.Utility
import UtilityPackage.Utility.createKafkaProducer
import com.bridgelabz.PythonHandlerPackage.PythonHandler
import com.bridgelabz.TwitterSentimentAnalysisPackage.TwitterStreamingAnalysis
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mockito.Mockito.{verify, when}
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.collection.mutable

class TwitterSentimentAnalysisTest extends FunSuite with Serializable {
  val sparkSessionObj: SparkSession =
    Utility.createSessionObject("Twitter Sentimental Analysis Test")
  import sparkSessionObj.implicits._
  val pythonHandlerObj = new PythonHandler(sparkSessionObj)

  val twitterStreamingAnalysisObj =
    new TwitterStreamingAnalysis(sparkSessionObj, pythonHandlerObj)
  val tweet =
    "Check out my Gig on Fiverr: stencil art https://t.co/9dYg70kWNj\n\n#RepublicansForBiden #Capricorn #Philadelphia\u2026 https://t.co/IsBhlMViBK"
  val tweets = List(tweet)
  val frameComparisonObj = new FrameComparison()
  val inputDataFrame: DataFrame = tweets.toDF("value")
  val wrongColumnDataFrame: DataFrame = tweets.toDF("values")
  val filePath = "./PythonFiles/SentimentAnalysis.py"
  val wrongPythonFilePath = "./PythonFiles/SentmentAnalysis.py"
  val cleanedTweetsDataFrame =
    twitterStreamingAnalysisObj.preProcessingTweets(inputDataFrame)

  sparkSessionObj.sparkContext.setLogLevel("ERROR");
  val brokers = "localhost:9092"
  val topics = "TwitterTest"
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

  def cleansingTweets(inputDataFrame: DataFrame): DataFrame = {
    val tokenizedTweetDataFrame = inputDataFrame
      .select("value")
      .withColumn("Tweets", col("value").cast(StringType))
      .withColumn(
        "TweetsCleaned",
        extractingTextPartUDF(col("Tweets"))
      )
      .withColumn("TweetsTokenized", tokenizerUDF(col("TweetsCleaned")))
    tokenizedTweetDataFrame
  }

  def preProcessingTweets(inputDataFrame: DataFrame): DataFrame = {
    //Removing special characters and numbers from Tweets and tokenizing it
    val tokenizedTweetDataFrame = cleansingTweets(inputDataFrame)
    val cleanedTweetsDataFrame = removeStopWords(tokenizedTweetDataFrame)
    cleanedTweetsDataFrame
  }

  def removeStopWords(inputDataFrame: DataFrame): DataFrame = {
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

  test(
    "test_preProcessingTweets_RemovingUndesiredCharacter_ReturnCleanedTweetsDataFrame"
  ) {
    val outputDataFrame =
      twitterStreamingAnalysisObj.preProcessingTweets(inputDataFrame)
    val outputDataFrameTest = preProcessingTweets(inputDataFrame)
    assert(
      frameComparisonObj.frameComparison(outputDataFrame, outputDataFrameTest)
    )

  }
  test(
    "test_preProcessingTweets_RemovingUndesiredCharacter_PassWrongColumnName_ThrowException"
  ) {
    val thrown = intercept[Exception] {
      val _ =
        twitterStreamingAnalysisObj.preProcessingTweets(wrongColumnDataFrame)
    }
    assert(thrown.getMessage == "Spark Sql Analysis Exception")
  }
  test(
    "test_performingSentimentalAnalysis_ProvidingPositiveTweet_ReturnDataFrameWithPositivePolarity"
  ) {
    val polarityScoreOfReviewsRDD =
      Utility.runPythonCommand(
        filePath,
        cleanedTweetsDataFrame.select("CleanedTweets")
      )
    val polarityScores =
      polarityScoreOfReviewsRDD
        .collect()
        .toList
        .map(elements => elements)
    //Creating a new dataframe with new predicted value Column
    val polarityScoreOfReviewsDataFrameTest = sparkSessionObj.createDataFrame(
      // Adding New Column
      cleanedTweetsDataFrame.rdd.zipWithIndex.map {
        case (row, columnIndex) =>
          Row.fromSeq(row.toSeq :+ polarityScores(columnIndex.toInt))
      },
      // Create schema
      StructType(
        cleanedTweetsDataFrame.schema.fields :+ StructField(
          "PolarityScore",
          StringType,
          false
        )
      )
    )

    val polarityScoreOfReviewsDataFrame =
      pythonHandlerObj.performingSentimentAnalysis(
        cleanedTweetsDataFrame,
        filePath
      )
    assert(
      frameComparisonObj.frameComparison(
        polarityScoreOfReviewsDataFrameTest,
        polarityScoreOfReviewsDataFrame
      )
    )
  }
  test(
    "test_performSentimentAnalysis_ProvidingWrongColumnDataFrame_ThrowException"
  ) {
    val thrown = intercept[Exception] {
      val _ =
        pythonHandlerObj.performingSentimentAnalysis(
          inputDataFrame,
          filePath
        )
    }
    assert(thrown.getMessage == "Spark Sql Analysis Exception")
  }
  test(
    "test_performSentimentAnalysis_ProvidingWrongPythonFilePath_ThrowException"
  ) {
    val thrown = intercept[Exception] {
      val _ =
        pythonHandlerObj.performingSentimentAnalysis(
          inputDataFrame,
          wrongPythonFilePath
        )
    }
    assert(thrown.getMessage == "Spark Sql Analysis Exception")
  }
  test(
    "test_getSentimentScoreFunction_Checking_Whether_performingSentimentAnalysisFunctionisCalledOrNot_ReturnsTrue"
  ) {
    val pythonHandlerService = mock[PythonHandler]
    val returnedDataFrame =
      pythonHandlerObj.performingSentimentAnalysis(
        cleanedTweetsDataFrame,
        filePath
      )
    when(
      pythonHandlerService
        .performingSentimentAnalysis(cleanedTweetsDataFrame, filePath)
    ).thenReturn(returnedDataFrame)
    val twitterStreamingAnalysisMockObj =
      new TwitterStreamingAnalysis(sparkSessionObj, pythonHandlerService)

    twitterStreamingAnalysisMockObj.getSentimentScore(
      cleanedTweetsDataFrame,
      filePath
    )

    verify(pythonHandlerService)
      .performingSentimentAnalysis(cleanedTweetsDataFrame, filePath)
  }

  test(
    "test_TakingInputFromKafka_DirectlyTakingInputFromKafkaProducer_UsingTakingInputFunction_ReturnsTrueForFrameComparison"
  ) {
    val kafkaProducer = createKafkaProducer(brokers)
    val preProcessedInputTestDF = preProcessingTweets(inputDataFrame)
    def checkDataFrames(batchDF: DataFrame): Unit = {
      val preProcessedInputDF =
        twitterStreamingAnalysisObj.preProcessingTweets(batchDF)
      preProcessedInputDF.show(1)
      assert(
        frameComparisonObj
          .frameComparison(preProcessedInputTestDF, preProcessedInputDF)
      )
    }

    val record =
      new ProducerRecord[String, String](
        topics,
        "",
        tweet
      )
    kafkaProducer.send(record)
    val functionDataFrame =
      twitterStreamingAnalysisObj.takingInputFromKafka(brokers, topics)

    functionDataFrame.writeStream
      .format("console")
      .foreachBatch((batchDF: DataFrame, _: Long) => checkDataFrames(batchDF))
      .start()
      .awaitTermination(20000)
  }
//  test(
//    "test_TakingInputFromKafka_DirectlyTakingInputFromKafkaProducer_UsingTakingInputFunction_ReturnsException"
//  ) {
//    val kafkaProducer = createKafkaProducer(brokers)
//    val record =
//      new ProducerRecord[String, String](
//        topics,
//        "",
//        tweet
//      )
//    kafkaProducer.send(record)
//    val thrown = intercept[Exception] {
//      val functionDataFrame =
//        twitterStreamingAnalysisObj.takingInputFromKafka("hjk", topics)
//      val query = functionDataFrame.writeStream
//        .format("console")
//        .queryName("Real Time Stock Prediction Query")
//        .start()
//      query.awaitTermination(300000)
//    }
//    assert(
//      thrown.getMessage == "Failed to construct kafka consumer"
//    )
//  }
}
