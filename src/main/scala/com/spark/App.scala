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
    //parte 1
    val df_1 = spark.read.options(Map("inferSchema"->"true","header"->"true")).csv("/home/ruicorreia/Documents/Spark/google-play-store-apps/googleplaystore_user_reviews.csv")
      .select("App","Sentiment_Polarity")
      .groupBy("App")
      .agg(avg("Sentiment_Polarity")
        .cast(DoubleType).as("Average_Sentiment_Polarity"))
      .na.fill(0)
      df_1.show()
    //parte 2 not null e not NaN for style points 
    val df_2 = spark.read.options(Map("inferSchema"->"true","header"->"true"))
      .csv("/home/ruicorreia/Documents/Spark/google-play-store-apps/googleplaystore.csv") 
      .filter(org.apache.spark.sql.functions.col("Rating").cast("double") >=4.0 && org.apache.spark.sql.functions.col("Rating").isNotNull && !org.apache.spark.sql.functions.col("Rating").isNaN)
      .orderBy(org.apache.spark.sql.functions.col("Rating").desc)
    df_2.show()
    df_2.write.options(Map("header"->"true", "delimiter"->"§")).csv("best_apps.csv")
  }
}
