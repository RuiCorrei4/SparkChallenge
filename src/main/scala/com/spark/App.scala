package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.avro.data.Json




/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    
    //creats conection to the spark "server"
    val spark = SparkSession.builder()
      .master( master = "local[*]")
      .getOrCreate()
    //fixes to_date idk y?
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

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

    //parte 3
    val df_3 = spark.read.options(Map("inferSchema"->"true","header"->"true"))
      .option("delimiter", ",")
      .csv("/home/ruicorreia/Documents/Spark/google-play-store-apps/googleplaystore.csv")
      .withColumn("Genres", split(col("Genres"), ";").cast("array<string>"))
      .withColumn("Reviews", col("Reviews").cast("long"))
      .withColumn("Price", regexp_replace(col("Price"), "\\$", "").cast("double")*0.9)
      .withColumn("Size", regexp_replace(col("Size"), "M", "000000000"))
      .withColumn("Size", regexp_replace(col("Size"), "\\.", "").cast("double"))
      .withColumn("Content_Rating", col("Content Rating")).drop("Content Rating")
      .withColumn("Last Updated", regexp_replace(col("Last Updated"), "\\,", ""))
      .withColumn("Last_Updated", to_date(col("Last Updated"),"MMMM dd yyyy").cast("date")).drop("Last Updated")
      .withColumn("Current_Ver", col("Current Ver")).drop("Current Ver")
      .withColumn("Android_Ver", col("Android Ver")).drop("Android Ver")
      .groupBy("App","Rating","Size", "Installs","Type","Price","Content_Rating","Genres","Last_Updated","Current_Ver","Android_Ver")
      .agg(array_distinct(
        collect_list("Category")).as("Categories"), 
        max("Reviews").as("Reviews")
      ).na.fill(0,Array("Reviews"))
      .na.fill("",Array("App"))
      .na.fill("",Array("Categories"))
      .na.fill("null")
      df_3.show(false)
      df_3.printSchema()

      //parte 4

  }
}
