#!/bin/bash
spark-submit --driver-java-options "-Dlog4j.configuration=log4j.properties" --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --class SparkStructuredStreaming.StockPredictionKafkaStructuredStreaming "target/scala-2.12/stockpredictionstructuredstreaming_2.12-0.1.jar" "65.0.103.18:9092" "kafkatutorial" "./SavedOutput" "pythonFiles/StockPricePrediction.py"