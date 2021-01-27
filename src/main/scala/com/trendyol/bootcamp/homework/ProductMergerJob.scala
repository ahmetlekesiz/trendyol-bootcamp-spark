package com.trendyol.bootcamp.homework

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.trendyol.bootcamp.batch.ProductViewEvent
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, max, row_number}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try


// This case class defines our data record type
case class ProductData ( id: Long, name: String, category: String, brand: String, color: String,
                         price: Double, timestamp: Long)

object ProductMergerJob {

  def initSpark(): SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark ")
      .getOrCreate()
    spark
  }

  def generateCurrentDate(): String = {
    val yyyyMMddFormatter    = DateTimeFormatter.ofPattern("yyyyMMdd")
    val currentTime          = LocalDateTime.now().truncatedTo(ChronoUnit.DAYS)
    val formattedCurrentDate = currentTime.format(yyyyMMddFormatter)
    formattedCurrentDate
  }

  def mergeProductDatasets(initialDataset: Dataset[ProductData], newDataset: Dataset[ProductData]): DataFrame = {
    // Concat two dataset
    val initialAndNewDataset = initialDataset.union(newDataset)

    // Get updated records by using Window
    // I have ordered by timestamp to use during filter to get the most recent record of the product.
    val win = Window.partitionBy("id").orderBy(org.apache.spark.sql.functions.col("timestamp").desc)

    // Set row_numbers to each record.
    // I have filtered the rows which are less than 2 in record column since the most recent record will have 1 in result column.
    val filteredDataset = initialAndNewDataset
      .withColumn("result",row_number().over(win))
      .filter(col("result")<2)
      .drop("result")
      .orderBy("id")

    // Rearrange the column order
    val updatedDataframe = filteredDataset.select("id", "name", "category", "brand", "color", "price", "timestamp")
    updatedDataframe
  }

  def saveUpdatedDataframe(returnDataset: DataFrame, formattedCurrentDate: String): Unit = {
    returnDataset
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .json("homework_output/batch1")
  }

  def main(args: Array[String]): Unit = {

    /***  HOMEWORK DEFINITION
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

    /***
    * ASSUMPTION
    * I assume that this job will run once in a day. In this assumption there is only one json file in each partition_date folder.
    * If it needs to be run more than one in a day, we need to get latest json in the partition_date folder.
    * -------------------
    * SOLUTION APPROACH
    * First, look at the batch output folder. If there is any exist data, read json as dataset and use it for merging with new dataset.
    * If there is no exist data, get initial data and use it for merging with new dataset.
    */

    // Init spark session
    val spark = initSpark()

    // Change the Log Level to see just Errors.
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // Use initialStarTime when running the job for first time
    val initialStartTime     = "20210127"

    // Get current data for using while updating dataset after first time
    val formattedCurrentDate = generateCurrentDate()

    /*
    // Try to get previous output, if you are not running the job for the first time
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val lastPartitionFolderName = Try(fs.listStatus(new Path(s"homework_output/batch")).filter(_.isDir).map(_.getPath).map(e => (e.toString).split("/").last).last).getOrElse("")
    val lastPartitionFolderPath = "homework_output/batch/" + lastPartitionFolderName
*/
    val viewSchema = Encoders.product[ProductData].schema

    // TODO
    val lastProcessedDataset = Try(
      spark.read.schema(viewSchema)
        .json("homework_output/batch").as[ProductData]
    ).getOrElse(spark.emptyDataset[ProductData])

    // Read initial dataset
   // val cdcDataset = spark.read.json(s"data/homework/initial_data.json").as[ProductData]

    val cdcDataset = spark.read.schema(viewSchema).json(s"data/homework/cdc_data.json").as[ProductData]

    //val cdcDataset = spark.read.json(s"data/homework/cdc_data.json").as[ProductData]

    // Read new dataset
    //val newDataset = spark.read.json(s"data/homework/cdc_data.json").as[ProductData]

    val returnDataframe = mergeProductDatasets(cdcDataset, lastProcessedDataset)
    saveUpdatedDataframe(returnDataframe, formattedCurrentDate)

    /*
    // Merge two dataset and return updated version as dataframe
    if(formattedCurrentDate == initialStartTime) {
      // Save the new dataset.
      val returnDataframe = mergeProductDatasets(initialDataset, newDataset)
      saveUpdatedDataframe(returnDataframe, initialStartTime)
    }else {
      // Save the updated dataset.
      val returnDataframe = mergeProductDatasets(lastProcessedDataset, newDataset)
      saveUpdatedDataframe(returnDataframe, formattedCurrentDate)
    }
     */

  }

}
