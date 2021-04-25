package com.coding.challenge.tier

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object LoadTrackEventsData {
  def main(args: Array[String]): Unit = {
    
    if (args.isEmpty || args.length > 2){
      println("Please provide the arguments as track_events <file-location> and table location <hdfs / s3>")  
      System.exit(0)
    }
  
  // Obtain Spark Session and related configurations.
    val spark = SparkSession
      .builder()
      .appName("Load-TrackEvents-Json")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.master("local[*]")
      .getOrCreate()
      
      import spark.implicits._
      
  // Read Json Events data.
      val events_data = spark.read.json(args(0))
      .drop("_corrupt_record")
      .withColumnRenamed("segment_tracks.anonymous_id", "anonymous_id")
      .withColumnRenamed("segment_tracks.context_app_build", "context_app_build")
      .withColumnRenamed("segment_tracks.context_app_name", "context_app_name")
      .withColumnRenamed("segment_tracks.context_app_version", "context_app_version")
      .withColumnRenamed("segment_tracks.context_device_id", "context_device_id")
      .withColumnRenamed("segment_tracks.context_device_manufacturer", "context_device_manufacturer")
      .withColumnRenamed("segment_tracks.context_device_model", "context_device_model")
      .withColumnRenamed("segment_tracks.context_device_type", "context_device_type")
      .withColumnRenamed("segment_tracks.context_os_name", "context_os_name")
      .withColumnRenamed("segment_tracks.context_os_version", "context_os_version")
      .withColumnRenamed("segment_tracks.context_timezone", "context_timezone")
      .withColumnRenamed("segment_tracks.event_name", "event_name")
      .withColumnRenamed("segment_tracks.original_timestamp_time", "original_timestamp_time")
      .withColumnRenamed("segment_tracks.properties_rating", "properties_rating")
      .withColumnRenamed("segment_tracks.received_time", "received_time")
      .withColumnRenamed("segment_tracks.sent_time", "sent_time")
      .withColumn("region",split(col("context_timezone"), "/").getItem(0))
      .withColumn("market_area",split(col("context_timezone"), "/").getItem(1))
      .withColumn("processed_date", current_date())
      
      //events_data.printSchema()
      
      events_data.select(
          "anonymous_id", 
          "context_app_build", 
          "context_app_name", 
          "context_app_version", 
          "context_device_id",
          "context_device_manufacturer",
          "context_device_model",
          "context_device_type",
          "context_os_name",
          "context_os_version",
          "context_timezone",
          "event_name",
          "original_timestamp_time",
          "properties_rating",
          "received_time",
          "sent_time",
          "region",
          "market_area",
          "processed_date")
         .write
         .mode("Overwrite")
         .partitionBy("processed_date", "region", "market_area")
         .parquet(args(1))
        
      print("Process completed, loading to Track-Events table !!")
  
      spark.stop()
      
  }
}