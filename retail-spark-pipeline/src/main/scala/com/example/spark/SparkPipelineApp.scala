package com.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import com.rabbitmq.client.ConnectionFactory
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime

object SparkPipelineApp {
  def main(args: Array[String]): Unit = {
    // 🛡️ Robust argument parsing: handles ProcessBuilder ("1,filename"), direct filename ("filename.csv"), or empty defaults
    val (fileId, filesToProcess) = if (args.nonEmpty) {
      val arg0 = args(0).trim
      if (arg0.contains(",")) {
        val parts = arg0.split(",")
        val id = try { parts(0).trim.toLong } catch { case _: Exception => 1L }
        val filename = if (parts.length > 1) parts(1).trim else "retail_customers_messy.csv"
        (id, Array(filename))
      } else if (arg0.endsWith(".csv")) {
        (1L, Array(arg0))
      } else {
        (1L, Array("retail_customers_messy.csv", "retail_orders_messy.csv"))
      }
    } else {
      (1L, Array("retail_customers_messy.csv", "retail_orders_messy.csv"))
    }

    // 🚀 Spark Session initialized with local[*] execution mode
    val spark = SparkSession.builder()
      .appName("Retail-Medallion-Spark-Pipeline-MultiFile")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Azure Blob Storage configuration
    val storageAccountName = "filestorageaccount1997"
    val accountKey = "JNwiedU79cBPdCsiLJSDnceDfqrUoRg5dwhoFdcq3y1/NFeDAKHAGdrq9TRzITZbTyjdj/DTgs3q+AStFpT10A=="

    sc.hadoopConfiguration.set(
      s"fs.azure.account.key.$storageAccountName.blob.core.windows.net",
      accountKey
    )

    println(s"🚀 Spark Session initialized. Processing file ID: $fileId (${filesToProcess.length} file(s))...")

    val containerName = "files-artifact-container"
    val baseUrl = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net"

    var allSuccessful = true

    // Initialize Hadoop FileSystem to programmatically locate UUID-prefixed blobs
    val conf = sc.hadoopConfiguration
    val rootPath = new Path(baseUrl + "/")
    val fs = rootPath.getFileSystem(conf)

    // Loop through each file and process into the Bronze layer
    for (targetFileName <- filesToProcess) {
      val trimmedFileName = targetFileName.trim

      println(s"\n========================================")
      println(s"📥 [BATCH START] Looking up file in Azure: $trimmedFileName")

      try {
        var exactFilePath: String = null

        if (fs.exists(rootPath)) {
          val statuses = fs.listStatus(rootPath)
          println(s"📂 Container root currently contains ${statuses.length} file(s):")
          statuses.foreach(s => println(s"   - ${s.getPath.getName}"))

          // Programmatically find the exact blob name ending with our target CSV name (handling UUID prefix)
          val matchedStatus = statuses.find(s => s.getPath.getName.endsWith(trimmedFileName))
          if (matchedStatus.isDefined) {
            exactFilePath = matchedStatus.get.getPath.toString
          }
        }

        if (exactFilePath == null) {
          throw new java.io.FileNotFoundException(
            s"Could not find any blob ending with '$trimmedFileName' in Azure container '$containerName'. " +
              s"Please ensure the file has been uploaded via your Spring Boot API."
          )
        }

        println(s"🔍 Resolved Exact Blob Path -> $exactFilePath")

        // 1. Read Raw CSV with inferSchema disabled (True Bronze Ingestion)
        val rawDf = spark.read
          .option("header", "true")
          .option("inferSchema", "false")
          .csv(exactFilePath)

        // 2. Enrich with audit and lineage metadata
        val bronzeDf = rawDf
          .withColumn("_ingestion_timestamp", current_timestamp())
          .withColumn("_source_file", lit(trimmedFileName))

        // 📊 Log row count and schema
        val recordCount = bronzeDf.count()
        println(s"📊 [LOG] Total Records Ingested: $recordCount")
        println(s"📋 [LOG] Bronze Schema for $trimmedFileName:")
        bronzeDf.printSchema()

        // 👀 Print a sample preview of the dataframe rows
        println(s"👀 [LOG] Dataframe Sample Preview (First 5 rows):")
        bronzeDf.show(5, truncate = false)

        // 3. Write to Bronze Parquet storage path
        val dataSetName = if (trimmedFileName.contains("customer")) "customers" else "orders"
        val bronzeOutputPath = s"./bronze_output/$dataSetName"

        println(s"💾 [LOG] Writing Parquet dataset to: $bronzeOutputPath")
        bronzeDf.write
          .mode("overwrite")
          .parquet(bronzeOutputPath)

        println(s"✅ [BATCH SUCCESS] Saved to Parquet: $bronzeOutputPath")

      } catch {
        case e: Exception =>
          allSuccessful = false
          println(s"❌ [BATCH ERROR] Failed processing file $trimmedFileName: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    // 4. Send Completion Event back to RabbitMQ for the Orchestrator
    if (allSuccessful) {
      try {
        val factory = new ConnectionFactory()
        factory.setHost("localhost")
        factory.setPort(5672)
        factory.setUsername("guest")
        factory.setPassword("guest")

        val connection = factory.newConnection()
        val channel = connection.createChannel()

        val exchangeName = "retail.bronze.completed.exchange"
        val routingKey = "bronze.completed"

        val eventMessage =
          s"""{
             |  "id": $fileId,
             |  "fileName": "${filesToProcess.head}",
             |  "blobUrl": "$baseUrl/",
             |  "currentStage": "BRONZE",
             |  "timestamp": "${LocalDateTime.now()}"
             |}""".stripMargin

        channel.basicPublish(exchangeName, routingKey, null, eventMessage.getBytes(StandardCharsets.UTF_8))
        println(s"\n📤 [RABBITMQ] Published Bronze completion event for file ID: $fileId to exchange '$exchangeName'")

        channel.close()
        connection.close()
      } catch {
        case e: Exception =>
          println(s"⚠️ [RABBITMQ WARNING] Failed to publish completion event: ${e.getMessage}")
      }
    }

    println(s"\n========================================")
    println("🏁 Pipeline execution finished!")
    spark.stop()
  }
}