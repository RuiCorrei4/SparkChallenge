package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    /**
      * creats conection to the spark "server"
      */
    val spark = SparkSession.builder()
      .master( master = "local[*]")
      .getOrCreate()
    //import csv
    val df_0 = spark.read.options(Map("inferSchema"->"true","header"->"true")).csv("/home/ruicorreia/Documents/Spark/google-play-store-apps/googleplaystore_user_reviews.csv")
    //part 1 
    val df_1 = df_0.select("App","Sentiment_Polarity").groupBy("App").agg(avg("Sentiment_Polarity").cast(DoubleType).as("Average_Sentiment_Polarity")).na.fill(0)
  }

}
