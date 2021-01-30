package com.trendyol.bootcamp.homework

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.trendyol.bootcamp.batch.{ChannelCategoryView, ProductViewEvent}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, max, row_number}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.reflect.io.Directory
import java.io.File
import java.nio.file.{Files, Path, StandardCopyOption}

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

  def mergeProductDatasets(initialDataset: Dataset[ProductData], newDataset: Dataset[ProductData]): DataFrame = {
    // Concat two dataset
    val initialAndNewDataset = initialDataset.union(newDataset)

    initialAndNewDataset.groupByKey(_.id)()
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

  def deleteDirectory(sourcePath: String): Unit = {
    val directory = new Directory(new File(sourcePath))
    directory.deleteRecursively()
  }

  def moveTempToSource(tempPath: String, sourcePath: String) = {
    val d1 = new File(tempPath).toPath
    val d2 = new File(sourcePath).toPath
    Files.move(d1, d2, StandardCopyOption.ATOMIC_MOVE)
  }

  def writeToTemp(returnDataset: DataFrame, tempPath: String) = {
    returnDataset
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(tempPath)
  }

  def saveUpdatedDataframe(returnDataset: DataFrame): Unit = {
    val sourcePath = "homework_output/batch"
    val tempPath = "homework_output/batch_temp"
    // Write to temp directory
    writeToTemp(returnDataset, tempPath)
    // Delete the source directory
    deleteDirectory(sourcePath)
    // Move temp to source directory
    moveTempToSource(tempPath, sourcePath)
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

    val viewSchema = Encoders.product[ProductData].schema

    val lastProcessedDataset = Try(
      spark.read.schema(viewSchema)
        .json("homework_output/batch").as[ProductData]
    ).getOrElse(spark.emptyDataset[ProductData])

    // If the output folder is empty, then read initial data, else read cdc_data.
    val cdcDataset = if(lastProcessedDataset.isEmpty) spark.read.schema(viewSchema).json(s"data/homework/initial_data.json").as[ProductData]
                      else spark.read.schema(viewSchema).json(s"data/homework/cdc_data.json").as[ProductData]

    // Merge two dataset and return updated version as dataframe
    val returnDataframe = mergeProductDatasets(cdcDataset, lastProcessedDataset)

    spark.catalog.clearCache()

    // Save the updated dataset.
    saveUpdatedDataframe(returnDataframe)
  }
}
