/**
  * @author Niraj Kumar
  */

package com.bridgelabz.TwitterSentimentAnalysisPackage

import plotly._
import Plotly._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{window}

/**
  * The objective of the class is to perform visualize the number of positive and negative tweets received.
  */
object PlottingGraphs extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate();
  spark.sparkContext.setLogLevel("ERROR")

  val twitterReviewsDf = spark.readStream
    .option("header", value = true)
    .option("inferSchema", value = true)
    .option("minFilesPerTrigger", 20)
    .csv(args(0))

  twitterReviewsDf.printSchema()

  val streamingCountsDF = (
    twitterReviewsDf
      .groupBy(
        twitterReviewsDf
          .col("PolarityScore"),
        window(
          twitterReviewsDf
            .col("Time"),
          "5 minute"
        )
      )
      .count()
    )

  def plotData(batchDF: DataFrame): Unit = {
    val (typeOfStatements, count) =
      batchDF.collect
        .map(r => (r(0).toString, r(2).toString.toInt))
        .toSeq
        .unzip
    Bar(typeOfStatements, count).plot()

  }

  streamingCountsDF.writeStream
    .format("console")
    .outputMode("update")
    .foreachBatch({ (batchDF: DataFrame, _: Long) => plotData(batchDF) })
    .queryName("Saving Output")
    .start()
    .awaitTermination()

}
