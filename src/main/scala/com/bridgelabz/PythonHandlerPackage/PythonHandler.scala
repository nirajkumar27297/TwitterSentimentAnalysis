package com.bridgelabz.PythonHandlerPackage

import UtilityPackage.Utility
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * The objective of the class is to call python script passed to its function and
  * perform sentimental analysis on the input dataframe contents.
  * @param sparkSessionObj
  */
class PythonHandler(sparkSessionObj: SparkSession) extends Serializable {
  //Configuring log4j
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /**
    * The function is used to run the python script provided as argument upon our dataframe
    * and perform sentimental analysis on our dataframe contents.
    * @param inputDataFrame [DataFrame]
    * @param filepath [String]
    * @return [DataFrame]
    */
  def performingSentimentAnalysis(
      inputDataFrame: DataFrame,
      filepath: String
  ): DataFrame = {
    try {
      logger.info("Performing Sentimental Analysis of Tweets Using Python NLTK")
      //Calling Utility class to run python script function
      val polarityScoreOfReviewsRDD = {
        Utility.runPythonCommand(
          filepath,
          inputDataFrame.select("CleanedTweets")
        )
      }
      // Storing polarity scores in a list
      val polarityScores =
        polarityScoreOfReviewsRDD
          .collect()
          .toList
      //Creating a new dataframe with new predicted value Column
      val polarityScoreOfReviewsDataFrame = sparkSessionObj.createDataFrame(
        // Adding New Column
        inputDataFrame.rdd.zipWithIndex.map {
          case (row, columnIndex) =>
            Row.fromSeq(row.toSeq :+ polarityScores(columnIndex.toInt))
        },
        // Create schema
        StructType(
          inputDataFrame.schema.fields :+ StructField(
            "PolarityScore",
            StringType,
            false
          )
        )
      )
      polarityScoreOfReviewsDataFrame
    } catch {
      case ex: org.apache.spark.sql.AnalysisException =>
        logger.info(
          "Difficulty in Sentimental Analysis of tweets Exception is" + ex
        )
        throw new Exception("Spark Sql Analysis Exception")
      case ex: Exception =>
        ex.printStackTrace()
        logger.info(
          "Difficulty in Sentimental Analysis of tweets Exception is" + ex
        )
        throw new Exception("Unexpected Error Occurred")
    }
  }

}
