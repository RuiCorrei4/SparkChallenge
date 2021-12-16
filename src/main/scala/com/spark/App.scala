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

   /* val customSchema = StructType(Array(
      StructField("App", StringType, true),
      StructField("Average_Sentiment_Polarity", DoubleType, true))
      )

    val df_1 = spark.read.format("csv")
      .option("delimiter"," ").option("quote","")
      .option("header", "true")
      .schema(customSchema)
      .load("/home/ruicorreia/Documents/Spark/google-play-store-apps/googleplaystore_user_reviews.csv")*/

   /* val data = spark.sparkContext.parallelize(Seq("Hello World", "This is some text", "Hello text"))

    val map = data.flatMap(e => e.split(" ")).map(word => (word, 1))

    val counts = map.reduceByKey(_ + _).repartition(1)

    //saves rdd
    counts.saveAsTextFile("wordcount")*/

    //saves a df as a csv file   
    df_1.write.csv("best_apps.csv")