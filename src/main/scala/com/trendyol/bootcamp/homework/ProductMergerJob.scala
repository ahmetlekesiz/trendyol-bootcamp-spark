package com.trendyol.bootcamp.homework

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, max, row_number}


// This case class defines our data record type
case class ProductData (
                         id: Long,
                         name: String,
                         category: String,
                         brand: String,
                         color: String,
                         price: Double,
                         timestamp: Long
                       )

object ProductMergerJob {

  def main(args: Array[String]): Unit = {

    /**
    * Find the latest version of each product in every run, and save it as snapshot.
    *
    * Product data stored under the data/homework folder.
    * Read data/homework/initial_data.json for the first run.
    * Read data/homework/cdc_data.json for the nex runs.
    *
    * Save results as json, parquet or etc.
    *
    * Note: You can use SQL, dataframe or dataset APIs, but type safe implementation is recommended.
    */

    // Init spark session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark ")
      .getOrCreate()

    val yyyyMMddFormatter    = DateTimeFormatter.ofPattern("yyyyMMdd")
    val currentTime          = LocalDateTime.now().truncatedTo(ChronoUnit.DAYS)
    val formattedCurrentDate = currentTime.format(yyyyMMddFormatter)

    // Change the Log Level to see just Errors.
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // Read initial dataset
    val ds = spark.read.json(s"data/homework/initial_data.json").as[ProductData]

    // Read new dataset
    val newDs = spark.read.json(s"data/homework/cdc_data.json").as[ProductData]

    // Concat two dataset
    val unionDs = ds.union(newDs)

    // Get updated records by using Window
    // I have ordered by timestamp to use during filter to get the most recent record of the product.
    val win = Window.partitionBy("id").orderBy(org.apache.spark.sql.functions.col("timestamp").desc)

    // Set row_numbers to each record.
    // I have filtered the rows which are less than 2 in record column since the most recent record will have 1 in result column.
    val mergedDs = unionDs
      .withColumn("result",row_number().over(win))
      .filter(col("result")<2)
      .drop("result")
      .orderBy("id")

    // Rearrange the column order
    val returnDs = mergedDs.select("id", "name", "category", "brand", "color", "price", "timestamp")

    // Save the updated dataset.
    returnDs
      .withColumn("partition_date", lit(formattedCurrentDate))
      .repartition(1)
      .write
      .partitionBy("partition_date")
      .mode(SaveMode.Append)
      .json("homework_output/batch")

  }

}
