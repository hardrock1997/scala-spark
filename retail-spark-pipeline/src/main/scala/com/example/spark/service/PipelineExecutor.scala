package com.example.spark.service

import com.example.spark.messaging.RabbitMQPublisher
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path

object PipelineExecutor {

  // 1. Initialize the SparkSession ONCE as a shared singleton for the application lifetime
  private val storageAccountName = "filestorageaccount1997"
  private val accountKey = "JNwiedU79cBPdCsiLJSDnceDfqrUoRg5dwhoFdcq3y1/NFeDAKHAGdrq9TRzITZbTyjdj/DTgs3q+AStFpT10A=="
  private val containerName = "files-artifact-container"
  val baseUrl: String = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net"

  val spark: SparkSession = {
    println("⚙️ [SPARK SESSION] Initializing global shared SparkSession...")
    val session = SparkSession.builder()
      .appName("Retail-Pipeline-Master")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1") // <--- Forces IPv4 binding
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    // Configure Hadoop Azure storage credentials globally on the shared Spark Context
    session.sparkContext.hadoopConfiguration.set(
      s"fs.azure.account.key.$storageAccountName.blob.core.windows.net",
      accountKey
    )

    session
  }

  def executeBronze(fileId: Long, targetFileName: String): Unit = {
    println(s"⚙️ [SPARK BRONZE] Processing File ID: $fileId, Target File: $targetFileName")
    var success = true
    val trimmedFileName = targetFileName.trim

    try {
      val rootPath = new Path(baseUrl + "/")
      val fs = rootPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
      var exactFilePath: String = null

      if (fs.exists(rootPath)) {
        fs.listStatus(rootPath).find(s => s.getPath.getName.endsWith(trimmedFileName)).foreach { matched =>
          exactFilePath = matched.getPath.toString
        }
      }

      if (exactFilePath == null) throw new java.io.FileNotFoundException(s"Blob ending with '$trimmedFileName' not found.")

      val rawDf = spark.read.option("header", "true").option("inferSchema", "false").csv(exactFilePath)
      val bronzeDf = rawDf
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", lit(trimmedFileName))

      println("--- BRONZE DF PREVIEW ---")

      bronzeDf.show(5, truncate = false)

      val dataSetName = if (trimmedFileName.contains("customer")) "customers" else "orders"
      val bronzeOutputPath = s"./bronze_output/$dataSetName"
      val bronzeCsvPath = s"./bronze_output/csv/$dataSetName"

      bronzeDf.write.mode("overwrite").parquet(bronzeOutputPath)
      bronzeDf.write
        .mode("overwrite")
        .option("header", "true") // Ensures column names are written to the CSV
        .csv(bronzeCsvPath)

      println(s"✅ [BRONZE] Saved Parquet dataset to: $bronzeOutputPath")
    } catch {
      case e: Exception =>
        success = false
        println(s"❌ [BRONZE ERROR]: ${e.getMessage}")
        e.printStackTrace()
    }

    if (success) {
      RabbitMQPublisher.publishEvent("retail.bronze.completed.exchange", "bronze.completed", fileId, targetFileName, baseUrl, "BRONZE")
    }
  }

  def executeSilver(fileId: Long, targetFileName: String): Unit = {
    println(s"⚙️ [SPARK SILVER] Processing Silver Cleansing for File ID: $fileId")
    var success = true
    val trimmedFileName = targetFileName.trim
    val isCustomer = trimmedFileName.contains("customer")
    val dataSetName = if (isCustomer) "customers" else "orders"
    val inputPath = s"./bronze_output/$dataSetName"
    val silverOutputPath = s"./silver_output/$dataSetName/$fileId"
    val silverCsvPath = s"./silver_output/csv/$dataSetName/$fileId"

    try {
      val bronzeDf = spark.read.parquet(inputPath)
      var cleanedDf = bronzeDf

      if (isCustomer) {
        cleanedDf = cleanedDf
          .na.replace("phone", Map("not_available" -> null))
          .na.replace("email", Map("not_available" -> null))
          .withColumn("gender",
            when(upper(col("gender")).isin("M", "MALE"), "Male")
              .when(upper(col("gender")).isin("F", "FEMALE"), "Female")
              .otherwise(lit(null)))
          .withColumn("status", initcap(trim(col("status"))))
          .withColumn("loyalty_points",
            when(col("loyalty_points") === "ten", lit(10))
              .otherwise(col("loyalty_points").cast(IntegerType))
          )
      } else {
        cleanedDf = cleanedDf
          .withColumn("quantity", when(col("quantity") === "abc" || col("quantity").isNull, lit(0)).otherwise(col("quantity").cast(IntegerType)))
          .withColumn("unit_price", when(col("unit_price") === "abc" || col("unit_price").isNull, lit(0.0)).otherwise(col("unit_price").cast(DoubleType)))
          .withColumn("order_status", initcap(trim(col("order_status"))))
          .withColumn("payment_method", initcap(trim(col("payment_method"))))
          .withColumn("quantity", abs(col("quantity")))
      }

      val finalDf = cleanedDf.withColumn("_silver_processed_timestamp", current_timestamp())
      finalDf.write.mode("overwrite").parquet(silverOutputPath)
      finalDf.write
        .mode("overwrite")
        .option("header", "true") // Ensures column names are written to the CSV
        .csv(silverCsvPath)
      println(s"✅ [SILVER COMPLETE] Written to $silverOutputPath")
    } catch {
      case e: Exception =>
        success = false
        println(s"❌ [SILVER ERROR]: ${e.getMessage}")
        e.printStackTrace()
    }

    if (success) {
      RabbitMQPublisher.publishEvent("retail.silver.completed.exchange", "silver.completed", fileId, targetFileName, baseUrl, "SILVER")
    }
  }

  def executeGold(fileId: Long, targetFileName: String): Unit = {
    println(s"⚙️ [SPARK GOLD] Processing Gold Aggregation for File ID: $fileId")
    var success = true
    val goldOutputPath = s"./gold_output/retail_summary_mart"
    val goldCsvPath = s"./gold_output/csv/retail_summary_mart"


    try {
      val customersDf = spark.read.parquet("./silver_output/customers/*/*")
      val ordersDf = spark.read.parquet("./silver_output/orders/*/*")

      val enrichedOrders = ordersDf.join(customersDf, "customer_id")
      val customerSummaryDf = enrichedOrders
        .groupBy("customer_id", "customer_name", "city", "state")
        .agg(
          count("order_id").as("total_orders"),
          sum("unit_price").as("total_spend"),
          avg("unit_price").as("avg_order_value")
        )
        .withColumn("_gold_processed_timestamp", current_timestamp())

      customerSummaryDf.write.mode("overwrite").parquet(goldOutputPath)
      customerSummaryDf.write
        .mode("overwrite")
        .option("header", "true") // Ensures column names are written to the CSV
        .csv(goldCsvPath)
      println(s"✅ [GOLD COMPLETE] Successfully generated retail analytical gold mart.")
    } catch {
      case e: Exception =>
        success = false
        println(s"❌ [GOLD ERROR]: ${e.getMessage}")
        e.printStackTrace()
    }

    if (success) {
      RabbitMQPublisher.publishEvent("retail.gold.completed.exchange", "gold.completed", fileId, targetFileName, baseUrl, "GOLD")
    }
  }
}