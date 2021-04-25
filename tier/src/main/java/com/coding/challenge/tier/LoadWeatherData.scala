package com.coding.challenge.tier

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object LoadWeatherData {
   def main(args: Array[String]): Unit = {
     
    if (args.isEmpty || args.length > 2){
      println("Please provide the arguments as weather-data <file-location> and table location <hdfs / s3>")  
      System.exit(0)
    }
     
     // Obtain Spark Session and related configurations.
    val spark = SparkSession
      .builder()
      .appName("Load-WeatherData-Json")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.master("local[*]")
      .getOrCreate()
      
      import spark.implicits._
      
    // Read Json Weather data. 
    val weather_data = spark.read.json(args(0))
    .withColumnRenamed("weather_data.city", "city")
    .withColumnRenamed("weather_data.currently_apparenttemperature", "currently_apparenttemperature")
    .withColumnRenamed("weather_data.currently_humidity", "currently_humidity")
    .withColumnRenamed("weather_data.currently_precipintensity", "currently_precipintensity")
    .withColumnRenamed("weather_data.currently_precipprobability", "currently_precipprobability")
    .withColumnRenamed("weather_data.currently_preciptype", "currently_preciptype")
    .withColumnRenamed("weather_data.currently_temperature", "currently_temperature")
    .withColumnRenamed("weather_data.currently_visibility", "currently_visibility")
    .withColumnRenamed("weather_data.currently_windspeed", "currently_windspeed")
    .withColumnRenamed("weather_data.date_time", "date_time")
    .withColumn("processed_date", current_date())
    
    
    weather_data.select(
        "city",
        "currently_apparenttemperature",
        "currently_humidity",
        "currently_precipintensity",
        "currently_precipprobability",
        "currently_preciptype",
        "currently_temperature",
        "currently_visibility",
        "currently_windspeed",
        "date_time",
        "processed_date")
        .write
        .mode("Overwrite")
        .partitionBy("processed_date", "city")
        .parquet(args(1))
        
   println("Process completed, loading to weather table!")

   spark.stop()
   }
}
