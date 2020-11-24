/*
Library Used
1> org.apache.spark.spark-core Version 3.0.0
--This is used for spark rdd operations
2> org.apache.spark.spark-sql Version 3.0.0
--This is used for dataframe operations
3> org.apache.spark.spark-streaming Version 3.0.0
--This is used for streaming operations
4> org.apache.spark.spark-mllib Version 3.0.0
--This is used for machine learning operations
5> org.apache.kafka.kafka-clients 2.6.0
--This is used for connecting kafka to spark and creating producer and consumer
6>org.apache.spark.spark-streaming-kafka-0.10 Version 2.4.0
--This is used for spark unstructured streaming
7> org.apache.spark.spark-sql-kafka-0.10 Version 3.0.0
--This is used for spark structured streaming
8> org.scalatest.scalatest Version 3.0.8
--This is used for unit testing.
9>   org.scalacheck.scalacheck Version 1.14.1
--This is used for unit testing.
10> org.mockito.mockito-all Version 1.8.4
-- This is used for mocking the services.
11> org.apache.hadoop.hadoop-common Version 3.2.1
12> org.apache.hadoop.hadoop-aws Version 3.2.1
-- The above jars are included for pushing file to S3 and configuring AWS.
13> jp.co.bizreach.aws-s3-scala Version 0.0.15
-- This is used to get cli for using performs various s3 operations in scala.
14> org.plotly-scala.plotly-render Version 0.8.0
-- This is used for visualization.

 */

name := "TwitterSentimentAnalysis"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.1" % "test"
libraryDependencies += "org.mockito" % "mockito-all" % "1.8.4"

// https://mvnrepository.com/artifact/org.plotly-scala/plotly-render
libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.8.0"
libraryDependencies += "jp.co.bizreach" %% "aws-s3-scala" % "0.0.15"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.1"
scapegoatVersion in ThisBuild := "1.3.8"
