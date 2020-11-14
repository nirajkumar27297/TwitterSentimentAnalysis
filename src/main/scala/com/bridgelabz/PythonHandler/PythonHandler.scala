package com.bridgelabz.PythonHandler

import UtilityPackage.Utility
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
class PythonHandler(sparkSessionObj: SparkSession) extends Serializable {
  //Configuring log4j
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def performingSentimentAnalysis(
      inputDataFrame: DataFrame,
      filepath: String
  ): DataFrame = {
    try {
      logger.info("Performing Sentimental Analysis of Tweets Using Python NLTK")
      val polarityScoreOfReviewsRDD =
        Utility.runPythonCommand(
          filepath,
          inputDataFrame.select("CleanedTweets")
        )
      val polarityScores =
        polarityScoreOfReviewsRDD
          .collect()
          .toList
          .map(elements => elements)
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
