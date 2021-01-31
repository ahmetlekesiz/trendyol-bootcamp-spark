package com.trendyol.bootcamp.homework

import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
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

  def mergeProductDatasets(spark: SparkSession, initialDataset: Dataset[ProductData], newDataset: Dataset[ProductData]): Dataset[ProductData] = {
    // TODO Global olarak import etmenin bir yolu var mı?
    //  Bu şekilde yaparsak her fonksiyona spark session'ı göndermemiz gerekecek.
    import spark.implicits._

    // Concat two dataset
    val initialAndNewDataset = initialDataset.union(newDataset)

    val updatedDataset = initialAndNewDataset
      .groupByKey(_.id)
      .reduceGroups{
        (a, b) => if(a.timestamp>b.timestamp) a else b
      }
      .map{
        case(id, product) =>  product
      }

    // Rearrange the column order
    updatedDataset
  }

  def deleteDirectory(sourcePath: String): Unit = {
    val directory = new Directory(new File(sourcePath))
    directory.deleteRecursively()
  }

  def moveTempToSource(tempPath: String, sourcePath: String) = {
    // TODO Use Hadoop File system
    //  Hadoop'u kullanmak çok daha mantıklı.
    //  Java file sistem kullanırsan localdeki path e bakıyor.

    val d1 = new File(tempPath).toPath
    val d2 = new File(sourcePath).toPath
    Files.move(d1, d2, StandardCopyOption.ATOMIC_MOVE)
  }

  def writeToTemp(returnDataset: Dataset[ProductData], tempPath: String) = {
    returnDataset
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(tempPath)
  }

  def saveUpdatedDataframe(returnDataset: Dataset[ProductData]): Unit = {
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
    val returnDataset = mergeProductDatasets(spark, cdcDataset, lastProcessedDataset)

    // Save the updated dataset.
    saveUpdatedDataframe(returnDataset)
  }
}
