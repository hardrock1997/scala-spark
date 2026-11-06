//package com.example.spark
//
//import com.rabbitmq.client._
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import org.apache.hadoop.fs.Path
//import java.nio.charset.StandardCharsets
//import java.time.LocalDateTime
//
//case class PipelineStageEvent(
//                               id: Long,
//                               fileName: String,
//                               blobUrl: String,
//                               currentStage: String,
//                               timestamp: String
//                             )
//
//object SparkPipelineConsumer {
//  def main(args: Array[String]): Unit = {
//    val RABBIT_HOST = "localhost"
//
//    // Bronze setup (Matching Orchestrator YAML)
//    val EXCHANGE_BRONZE_TRIGGER = "retail.bronze.trigger.exchange"
//    val QUEUE_BRONZE_TRIGGER = "retail.spark.bronze.trigger.queue"
//
//    // Silver setup (Matching Orchestrator YAML)
//    val EXCHANGE_SILVER_TRIGGER = "retail.silver.trigger.exchange"
//    val QUEUE_SILVER_TRIGGER = "retail.spark.silver.trigger.queue"
//
//    val factory = new ConnectionFactory()
//    factory.setHost(RABBIT_HOST)
//
//    val connection = factory.newConnection()
//    val channel = connection.createChannel()
//
//    // 1. Declare Bronze Trigger Bindings
//    channel.exchangeDeclare(EXCHANGE_BRONZE_TRIGGER, "topic", true)
//    channel.queueDeclare(QUEUE_BRONZE_TRIGGER, true, false, false, null)
//    channel.queueBind(QUEUE_BRONZE_TRIGGER, EXCHANGE_BRONZE_TRIGGER, "#")
//
//    // 2. Declare Silver Trigger Bindings (Listens for orchestrator's silver trigger exchange)
//    channel.exchangeDeclare(EXCHANGE_SILVER_TRIGGER, "topic", true)
//    channel.queueDeclare(QUEUE_SILVER_TRIGGER, true, false, false, null)
//    channel.queueBind(QUEUE_SILVER_TRIGGER, EXCHANGE_SILVER_TRIGGER, "#")
//
//    println(s"[*] Spark Pipeline Consumer daemon started.")
//    println(s"    - Waiting for Bronze triggers on queue '$QUEUE_BRONZE_TRIGGER'...")
//    println(s"    - Waiting for Silver triggers on queue '$QUEUE_SILVER_TRIGGER'...")
//
//    val mapper = new ObjectMapper()
//    mapper.registerModule(DefaultScalaModule)
//
//    val bronzeDeliverCallback: DeliverCallback = (consumerTag: String, delivery: Delivery) => {
//      val messageJson = new String(delivery.getBody, StandardCharsets.UTF_8)
//      println(s"🎯 [SPARK BRONZE CONSUMER] Received trigger: $messageJson")
//      try {
//        val event = mapper.readValue(messageJson, classOf[PipelineStageEvent])
//        runBronzePipeline(event.id, event.fileName)
//
//        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
//        println(s"✅ [SPARK BRONZE] Successfully processed file ID: ${event.id}")
//      } catch {
//        case e: Exception =>
//          println(s"❌ [SPARK BRONZE ERROR]: ${e.getMessage}")
//          e.printStackTrace()
//          channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
//      }
//    }
//
//    val silverDeliverCallback: DeliverCallback = (consumerTag: String, delivery: Delivery) => {
//      val messageJson = new String(delivery.getBody, StandardCharsets.UTF_8)
//      println(s"🎯 [SPARK SILVER CONSUMER] Received trigger: $messageJson")
//      try {
//        val event = mapper.readValue(messageJson, classOf[PipelineStageEvent])
//        runSilverPipeline(event.id, event.fileName)
//
//        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
//        println(s"✅ [SPARK SILVER] Successfully processed file ID: ${event.id}")
//      } catch {
//        case e: Exception =>
//          println(s"❌ [SPARK SILVER ERROR]: ${e.getMessage}")
//          e.printStackTrace()
//          channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
//      }
//    }
//
//    // Start consuming from both stage-specific queues concurrently
//    channel.basicConsume(QUEUE_BRONZE_TRIGGER, false, bronzeDeliverCallback, (_: String) => {})
//    channel.basicConsume(QUEUE_SILVER_TRIGGER, false, silverDeliverCallback, (_: String) => {})
//  }
//
//  def runBronzePipeline(fileId: Long, targetFileName: String): Unit = {
//    println(s"⚙️ [SPARK BRONZE] Initializing SparkSession for File ID: $fileId, Target File: $targetFileName")
//
//    val spark = SparkSession.builder()
//      .appName(s"Retail-Bronze-Pipeline-$fileId")
//      .master("local[*]")
//      .getOrCreate()
//
//    val sc = spark.sparkContext
//    val storageAccountName = "filestorageaccount1997"
//    val accountKey = "JNwiedU79cBPdCsiLJSDnceDfqrUoRg5dwhoFdcq3y1/NFeDAKHAGdrq9TRzITZbTyjdj/DTgs3q+AStFpT10A=="
//    sc.hadoopConfiguration.set(s"fs.azure.account.key.$storageAccountName.blob.core.windows.net", accountKey)
//
//    val containerName = "files-artifact-container"
//    val baseUrl = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net"
//    var allSuccessful = true
//    val trimmedFileName = targetFileName.trim
//
//    try {
//      val rootPath = new Path(baseUrl + "/")
//      val fs = rootPath.getFileSystem(sc.hadoopConfiguration)
//      var exactFilePath: String = null
//
//      if (fs.exists(rootPath)) {
//        val statuses = fs.listStatus(rootPath)
//        statuses.find(s => s.getPath.getName.endsWith(trimmedFileName)).foreach { matched =>
//          exactFilePath = matched.getPath.toString
//        }
//      }
//
//      if (exactFilePath == null) throw new java.io.FileNotFoundException(s"Blob ending with '$trimmedFileName' not found.")
//
//      println(s"🔍 [BRONZE STEP 1] Resolved Blob Path -> $exactFilePath")
//
//      val rawDf = spark.read.option("header", "true").option("inferSchema", "false").csv(exactFilePath)
//      println(s"📊 [BRONZE STEP 2] Read Raw CSV records count: ${rawDf.count()}")
//
//      val bronzeDf = rawDf
//        .withColumn("_ingestion_timestamp", current_timestamp())
//        .withColumn("_source_file", lit(trimmedFileName))
//      println(s"📊 [BRONZE STEP 3] Added audit lineage columns.")
//
//      val dataSetName = if (trimmedFileName.contains("customer")) "customers" else "orders"
//      val bronzeOutputPath = s"./bronze_output/$dataSetName"
//
//      bronzeDf.write.mode("overwrite").parquet(bronzeOutputPath)
//      println(s"✅ [BRONZE STEP 4] Saved Parquet dataset to: $bronzeOutputPath")
//
//    } catch {
//      case e: Exception =>
//        allSuccessful = false
//        println(s"❌ [BRONZE ERROR] Failed processing $trimmedFileName: ${e.getMessage}")
//        e.printStackTrace()
//    } finally {
//      spark.stop()
//    }
//
//    if (allSuccessful) {
//      publishEvent("retail.bronze.completed.exchange", "bronze.completed", fileId, targetFileName, baseUrl, "BRONZE")
//    }
//  }
//
////  def runSilverPipeline(fileId: Long, targetFileName: String): Unit = {
////    println(s"⚙️ [SPARK SILVER] Initializing Silver Cleansing SparkSession for File ID: $fileId, Target File: $targetFileName")
////
////    val spark = SparkSession.builder()
////      .appName(s"Retail-Silver-Cleansing-Pipeline-$fileId")
////      .master("local[*]")
////      .getOrCreate()
////
////    var allSuccessful = true
////    val trimmedFileName = targetFileName.trim
////    val isCustomer = trimmedFileName.contains("customer")
////    val dataSetName = if (isCustomer) "customers" else "orders"
////    val inputPath = s"./bronze_output/$dataSetName"
////
////    // Updated output path to include fileId to prevent parallel file job collisions
////    val silverOutputPath = s"./silver_output/$dataSetName/$fileId"
////
////    try {
////      // STEP 1: Read from Bronze Parquet
////      println(s"🔍 [SILVER STEP 1] Reading Bronze dataset from: $inputPath")
////      val bronzeDf = spark.read.parquet(inputPath)
////      println(s"📊 [SILVER STEP 1] Loaded records count: ${bronzeDf.count()}")
////
////      var cleanedDf = bronzeDf
////
////      if (isCustomer) {
////        // STEP 2 (Customers): Handle missing/invalid strings & 'not_available' placeholders[cite: 1]
////        println(s"🧹 [SILVER STEP 2] Cleaning customer text fields (replacing 'not_available' with null)...")
////        cleanedDf = cleanedDf
////          .na.replace("phone", Map("not_available" -> null))
////          .na.replace("email", Map("not_available" -> null))
////
////        // STEP 3 (Customers): Standardize Gender & Status casing[cite: 1]
////        println(s"🧹 [SILVER STEP 3] Standardizing customer gender and status fields...")
////        cleanedDf = cleanedDf
////          .withColumn("gender",
////            when(upper(col("gender")).isin("M", "MALE"), "Male")
////              .when(upper(col("gender")).isin("F", "FEMALE"), "Female")
////              .otherwise(lit(null)))
////          .withColumn("status", initcap(trim(col("status"))))
////
////        // STEP 4 (Customers): Fix text words in numeric columns (e.g., 'ten' -> 10)[cite: 1]
////        println(s"🧹 [SILVER STEP 4] Casting and cleansing customer loyalty points...")
////        cleanedDf = cleanedDf
////          .withColumn("loyalty_point",
////            when(col("loyalty_point") === "ten", lit(10))
////              .otherwise(col("loyalty_point").cast(IntegerType))
////          )
////
////      } else {
////        // STEP 2 (Orders): Handle missing quantities or text placeholders[cite: 2]
////        println(s"🧹 [SILVER STEP 2] Cleaning order quantities and unit prices...")
////        cleanedDf = cleanedDf
////          .withColumn("quantity",
////            when(col("quantity") === "abc" || col("quantity").isNull, lit(0))
////              .otherwise(col("quantity").cast(IntegerType))
////          )
////          .withColumn("unit_price",
////            when(col("unit_price") === "abc" || col("unit_price").isNull, lit(0.0))
////              .otherwise(col("unit_price").cast(DoubleType))
////          )
////
////        // STEP 3 (Orders): Clean order status & payment methods[cite: 2]
////        println(s"🧹 [SILVER STEP 3] Standardizing order status and store locations...")
////        cleanedDf = cleanedDf
////          .withColumn("order_status", initcap(trim(col("order_status"))))
////          .withColumn("payment_method", initcap(trim(col("payment_method"))))
////
////        // STEP 4 (Orders): Handle negative or anomaly quantities
////        println(s"🧹 [SILVER STEP 4] Correcting order anomalies (handling negative quantities)...")
////        cleanedDf = cleanedDf
////          .withColumn("quantity", abs(col("quantity")))
////      }
////
////      // STEP 5: Add Silver Audit Columns
////      println(s"📝 [SILVER STEP 5] Appending silver transformation timestamp lineage...")
////      val finalDf = cleanedDf.withColumn("_silver_processed_timestamp", current_timestamp())
////
////      // STEP 6: Write to Silver Parquet Partition/Path
////      println(s"💾 [SILVER STEP 6] Writing cleansed Silver dataset to: $silverOutputPath")
////      finalDf.write
////        .mode("overwrite")
////        .parquet(silverOutputPath)
////
////      println(s"✅ [SILVER COMPLETE] Successfully wrote cleansed dataset for $targetFileName to Silver layer.")
////
////    } catch {
////      case e: Exception =>
////        allSuccessful = false
////        println(s"❌ [SILVER ERROR] Failed processing silver pipeline for $targetFileName: ${e.getMessage}")
////        e.printStackTrace()
////    } finally {
////      spark.stop()
////      println(s"🛑 [SPARK SILVER] SparkSession stopped for file ID: $fileId")
////    }
////
////    // 7. Publish Silver Completion Event back to RabbitMQ for Gold stage tracking
////    if (allSuccessful) {
////      publishEvent("retail.silver.completed.exchange", "silver.completed", fileId, targetFileName, "wasbs://files-artifact-container@filestorageaccount1997.blob.core.windows.net/", "SILVER")
////    }
////  }
//
//
//  def runSilverPipeline(fileId: Long, targetFileName: String): Unit = {
//    println(s"⚙️ [SPARK SILVER] Initializing Silver Cleansing SparkSession for File ID: $fileId, Target File: $targetFileName")
//
//    val spark = SparkSession.builder()
//      .appName(s"Retail-Silver-Cleansing-Pipeline-$fileId")
//      .master("local[*]")
//      .getOrCreate()
//
//    var allSuccessful = true
//    val trimmedFileName = targetFileName.trim
//    val isCustomer = trimmedFileName.contains("customer")
//    val dataSetName = if (isCustomer) "customers" else "orders"
//    val inputPath = s"./bronze_output/$dataSetName"
//    val silverOutputPath = s"./silver_output/$dataSetName/$fileId"
//
//    try {
//      // STEP 1: Read from Bronze Parquet
//      println(s"🔍 [SILVER STEP 1] Reading Bronze dataset from: $inputPath")
//      val bronzeDf = spark.read.parquet(inputPath)
//      println(s"📊 [SILVER STEP 1] Loaded records count: ${bronzeDf.count()}")
//
//      var cleanedDf = bronzeDf
//
//      if (isCustomer) {
//        // STEP 2 (Customers): Handle missing/invalid strings & 'not_available' placeholders[cite: 1]
//        println(s"🧹 [SILVER STEP 2] Cleaning customer text fields (replacing 'not_available' with null)...")
//        cleanedDf = cleanedDf
//          .na.replace("phone", Map("not_available" -> null))
//          .na.replace("email", Map("not_available" -> null))
//
//        // STEP 3 (Customers): Standardize Gender & Status casing[cite: 1]
//        println(s"🧹 [SILVER STEP 3] Standardizing customer gender and status fields...")
//        cleanedDf = cleanedDf
//          .withColumn("gender",
//            when(upper(col("gender")).isin("M", "MALE"), "Male")
//              .when(upper(col("gender")).isin("F", "FEMALE"), "Female")
//              .otherwise(lit(null)))
//          .withColumn("status", initcap(trim(col("status"))))
//
//        // STEP 4 (Customers): Fix text words in numeric columns (loyalty_points plural)[cite: 1]
//        println(s"🧹 [SILVER STEP 4] Casting and cleansing customer loyalty points...")
//        cleanedDf = cleanedDf
//          .withColumn("loyalty_points",
//            when(col("loyalty_points") === "ten", lit(10))
//              .otherwise(col("loyalty_points").cast(IntegerType))
//          )
//
//      } else {
//        // STEP 2 (Orders): Handle missing quantities or text placeholders[cite: 2]
//        println(s"🧹 [SILVER STEP 2] Cleaning order quantities and unit prices...")
//        cleanedDf = cleanedDf
//          .withColumn("quantity",
//            when(col("quantity") === "abc" || col("quantity").isNull, lit(0))
//              .otherwise(col("quantity").cast(IntegerType))
//          )
//          .withColumn("unit_price",
//            when(col("unit_price") === "abc" || col("unit_price").isNull, lit(0.0))
//              .otherwise(col("unit_price").cast(DoubleType))
//          )
//
//        // STEP 3 (Orders): Clean order status & payment methods[cite: 2]
//        println(s"🧹 [SILVER STEP 3] Standardizing order status and store locations...")
//        cleanedDf = cleanedDf
//          .withColumn("order_status", initcap(trim(col("order_status"))))
//          .withColumn("payment_method", initcap(trim(col("payment_method"))))
//
//        // STEP 4 (Orders): Handle negative or anomaly quantities
//        println(s"🧹 [SILVER STEP 4] Correcting order anomalies (handling negative quantities)...")
//        cleanedDf = cleanedDf
//          .withColumn("quantity", abs(col("quantity")))
//      }
//
//      // STEP 5: Add Silver Audit Columns
//      println(s"📝 [SILVER STEP 5] Appending silver transformation timestamp lineage...")
//      val finalDf = cleanedDf.withColumn("_silver_processed_timestamp", current_timestamp())
//
//      // STEP 6: Write to Silver Parquet Partition/Path
//      println(s"💾 [SILVER STEP 6] Writing cleansed Silver dataset to: $silverOutputPath")
//      finalDf.write
//        .mode("overwrite")
//        .parquet(silverOutputPath)
//
//      println(s"✅ [SILVER COMPLETE] Successfully wrote cleansed dataset for $targetFileName to Silver layer.")
//
//    } catch {
//      case e: Exception =>
//        allSuccessful = false
//        println(s"❌ [SILVER ERROR] Failed processing silver pipeline for $targetFileName: ${e.getMessage}")
//        e.printStackTrace()
//    } finally {
//      spark.stop()
//      println(s"🛑 [SPARK SILVER] SparkSession stopped for file ID: $fileId")
//    }
//
//    // 7. Publish Silver Completion Event back to RabbitMQ for Gold stage tracking
//    if (allSuccessful) {
//      publishEvent("retail.silver.completed.exchange", "silver.completed", fileId, targetFileName, "wasbs://files-artifact-container@filestorageaccount1997.blob.core.windows.net/", "SILVER")
//    }
//  }
//  def publishEvent(exchange: String, routingKey: String, fileId: Long, targetFileName: String, baseUrl: String, stage: String): Unit = {
//    try {
//      val factory = new ConnectionFactory()
//      factory.setHost("localhost")
//      factory.setPort(5672)
//      factory.setUsername("guest")
//      factory.setPassword("guest")
//
//      val connection = factory.newConnection()
//      val channel = connection.createChannel()
//
//      channel.exchangeDeclare(exchange, "topic", true)
//
//      val eventMessage =
//        s"""{
//           |  "id": $fileId,
//           |  "fileName": "$targetFileName",
//           |  "blobUrl": "$baseUrl",
//           |  "currentStage": "$stage",
//           |  "timestamp": "${LocalDateTime.now()}"
//           |}""".stripMargin
//
//      channel.basicPublish(exchange, routingKey, null, eventMessage.getBytes(StandardCharsets.UTF_8))
//      println(s"📤 [RABBITMQ] Published $stage completion event for file ID: $fileId to exchange '$exchange'")
//
//      channel.close()
//      connection.close()
//    } catch {
//      case e: Exception =>
//        println(s"⚠️ [RABBITMQ WARNING] Failed to publish $stage completion event: ${e.getMessage}")
//    }
//  }
//}






package com.example.spark

import com.rabbitmq.client._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime

case class PipelineStageEvent(
                               id: Long,
                               fileName: String,
                               blobUrl: String,
                               currentStage: String,
                               timestamp: String
                             )

object SparkPipelineConsumer {
  def main(args: Array[String]): Unit = {
    val RABBIT_HOST = "localhost"

    // Bronze setup
    val EXCHANGE_BRONZE_TRIGGER = "retail.bronze.trigger.exchange"
    val QUEUE_BRONZE_TRIGGER = "retail.spark.bronze.trigger.queue"

    // Silver setup
    val EXCHANGE_SILVER_TRIGGER = "retail.silver.trigger.exchange"
    val QUEUE_SILVER_TRIGGER = "retail.spark.silver.trigger.queue"

    // Gold setup
    val EXCHANGE_GOLD_TRIGGER = "retail.gold.trigger.exchange"
    val QUEUE_GOLD_TRIGGER = "retail.spark.gold.trigger.queue"

    val factory = new ConnectionFactory()
    factory.setHost(RABBIT_HOST)

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    // 1. Declare Bronze Trigger Bindings
    channel.exchangeDeclare(EXCHANGE_BRONZE_TRIGGER, "topic", true)
    channel.queueDeclare(QUEUE_BRONZE_TRIGGER, true, false, false, null)
    channel.queueBind(QUEUE_BRONZE_TRIGGER, EXCHANGE_BRONZE_TRIGGER, "#")

    // 2. Declare Silver Trigger Bindings
    channel.exchangeDeclare(EXCHANGE_SILVER_TRIGGER, "topic", true)
    channel.queueDeclare(QUEUE_SILVER_TRIGGER, true, false, false, null)
    channel.queueBind(QUEUE_SILVER_TRIGGER, EXCHANGE_SILVER_TRIGGER, "#")

    // 3. Declare Gold Trigger Bindings
    channel.exchangeDeclare(EXCHANGE_GOLD_TRIGGER, "topic", true)
    channel.queueDeclare(QUEUE_GOLD_TRIGGER, true, false, false, null)
    channel.queueBind(QUEUE_GOLD_TRIGGER, EXCHANGE_GOLD_TRIGGER, "#")

    println(s"[*] Spark Pipeline Consumer daemon started.")
    println(s"    - Waiting for Bronze triggers on queue '$QUEUE_BRONZE_TRIGGER'...")
    println(s"    - Waiting for Silver triggers on queue '$QUEUE_SILVER_TRIGGER'...")
    println(s"    - Waiting for Gold triggers on queue '$QUEUE_GOLD_TRIGGER'...")

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val bronzeDeliverCallback: DeliverCallback = (consumerTag: String, delivery: Delivery) => {
      val messageJson = new String(delivery.getBody, StandardCharsets.UTF_8)
      println(s"🎯 [SPARK BRONZE CONSUMER] Received trigger: $messageJson")
      try {
        val event = mapper.readValue(messageJson, classOf[PipelineStageEvent])
        runBronzePipeline(event.id, event.fileName)
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
        println(s"✅ [SPARK BRONZE] Successfully processed file ID: ${event.id}")
      } catch {
        case e: Exception =>
          println(s"❌ [SPARK BRONZE ERROR]: ${e.getMessage}")
          e.printStackTrace()
          channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
      }
    }

    val silverDeliverCallback: DeliverCallback = (consumerTag: String, delivery: Delivery) => {
      val messageJson = new String(delivery.getBody, StandardCharsets.UTF_8)
      println(s"🎯 [SPARK SILVER CONSUMER] Received trigger: $messageJson")
      try {
        val event = mapper.readValue(messageJson, classOf[PipelineStageEvent])
        runSilverPipeline(event.id, event.fileName)
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
        println(s"✅ [SPARK SILVER] Successfully processed file ID: ${event.id}")
      } catch {
        case e: Exception =>
          println(s"❌ [SPARK SILVER ERROR]: ${e.getMessage}")
          e.printStackTrace()
          channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
      }
    }

    val goldDeliverCallback: DeliverCallback = (consumerTag: String, delivery: Delivery) => {
      val messageJson = new String(delivery.getBody, StandardCharsets.UTF_8)
      println(s"🎯 [SPARK GOLD CONSUMER] Received trigger: $messageJson")
      try {
        val event = mapper.readValue(messageJson, classOf[PipelineStageEvent])
        runGoldPipeline(event.id, event.fileName)
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
        println(s"✅ [SPARK GOLD] Successfully processed file ID: ${event.id}")
      } catch {
        case e: Exception =>
          println(s"❌ [SPARK GOLD ERROR]: ${e.getMessage}")
          e.printStackTrace()
          channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
      }
    }

    // Start consuming from all stage-specific queues concurrently
    channel.basicConsume(QUEUE_BRONZE_TRIGGER, false, bronzeDeliverCallback, (_: String) => {})
    channel.basicConsume(QUEUE_SILVER_TRIGGER, false, silverDeliverCallback, (_: String) => {})
    channel.basicConsume(QUEUE_GOLD_TRIGGER, false, goldDeliverCallback, (_: String) => {})
  }

  def runBronzePipeline(fileId: Long, targetFileName: String): Unit = {
    println(s"⚙️ [SPARK BRONZE] Initializing SparkSession for File ID: $fileId, Target File: $targetFileName")

    val spark = SparkSession.builder()
      .appName(s"Retail-Bronze-Pipeline-$fileId")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val storageAccountName = "filestorageaccount1997"
    val accountKey = "JNwiedU79cBPdCsiLJSDnceDfqrUoRg5dwhoFdcq3y1/NFeDAKHAGdrq9TRzITZbTyjdj/DTgs3q+AStFpT10A=="
    sc.hadoopConfiguration.set(s"fs.azure.account.key.$storageAccountName.blob.core.windows.net", accountKey)

    val containerName = "files-artifact-container"
    val baseUrl = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net"
    var allSuccessful = true
    val trimmedFileName = targetFileName.trim

    try {
      val rootPath = new Path(baseUrl + "/")
      val fs = rootPath.getFileSystem(sc.hadoopConfiguration)
      var exactFilePath: String = null

      if (fs.exists(rootPath)) {
        val statuses = fs.listStatus(rootPath)
        statuses.find(s => s.getPath.getName.endsWith(trimmedFileName)).foreach { matched =>
          exactFilePath = matched.getPath.toString
        }
      }

      if (exactFilePath == null) throw new java.io.FileNotFoundException(s"Blob ending with '$trimmedFileName' not found.")

      println(s"🔍 [BRONZE STEP 1] Resolved Blob Path -> $exactFilePath")

      val rawDf = spark.read.option("header", "true").option("inferSchema", "false").csv(exactFilePath)
      println(s"📊 [BRONZE STEP 2] Read Raw CSV records count: ${rawDf.count()}")

      val bronzeDf = rawDf
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", lit(trimmedFileName))
      println(s"📊 [BRONZE STEP 3] Added audit lineage columns.")

      val dataSetName = if (trimmedFileName.contains("customer")) "customers" else "orders"
      val bronzeOutputPath = s"./bronze_output/$dataSetName"

      bronzeDf.write.mode("overwrite").parquet(bronzeOutputPath)
      println(s"✅ [BRONZE STEP 4] Saved Parquet dataset to: $bronzeOutputPath")

    } catch {
      case e: Exception =>
        allSuccessful = false
        println(s"❌ [BRONZE ERROR] Failed processing $trimmedFileName: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }

    if (allSuccessful) {
      publishEvent("retail.bronze.completed.exchange", "bronze.completed", fileId, targetFileName, baseUrl, "BRONZE")
    }
  }

  def runSilverPipeline(fileId: Long, targetFileName: String): Unit = {
    println(s"⚙️ [SPARK SILVER] Initializing Silver Cleansing SparkSession for File ID: $fileId, Target File: $targetFileName")

    val spark = SparkSession.builder()
      .appName(s"Retail-Silver-Cleansing-Pipeline-$fileId")
      .master("local[*]")
      .getOrCreate()

    var allSuccessful = true
    val trimmedFileName = targetFileName.trim
    val isCustomer = trimmedFileName.contains("customer")
    val dataSetName = if (isCustomer) "customers" else "orders"
    val inputPath = s"./bronze_output/$dataSetName"
    val silverOutputPath = s"./silver_output/$dataSetName/$fileId"

    try {
      println(s"🔍 [SILVER STEP 1] Reading Bronze dataset from: $inputPath")
      val bronzeDf = spark.read.parquet(inputPath)
      println(s"📊 [SILVER STEP 1] Loaded records count: ${bronzeDf.count()}")

      var cleanedDf = bronzeDf

      if (isCustomer) {
        println(s"🧹 [SILVER STEP 2] Cleaning customer text fields...")
        cleanedDf = cleanedDf
          .na.replace("phone", Map("not_available" -> null))
          .na.replace("email", Map("not_available" -> null))

        println(s"🧹 [SILVER STEP 3] Standardizing customer gender and status fields...")
        cleanedDf = cleanedDf
          .withColumn("gender",
            when(upper(col("gender")).isin("M", "MALE"), "Male")
              .when(upper(col("gender")).isin("F", "FEMALE"), "Female")
              .otherwise(lit(null)))
          .withColumn("status", initcap(trim(col("status"))))

        println(s"🧹 [SILVER STEP 4] Casting and cleansing customer loyalty points...")
        cleanedDf = cleanedDf
          .withColumn("loyalty_points",
            when(col("loyalty_points") === "ten", lit(10))
              .otherwise(col("loyalty_points").cast(IntegerType))
          )
      } else {
        println(s"🧹 [SILVER STEP 2] Cleaning order quantities and unit prices...")
        cleanedDf = cleanedDf
          .withColumn("quantity",
            when(col("quantity") === "abc" || col("quantity").isNull, lit(0))
              .otherwise(col("quantity").cast(IntegerType))
          )
          .withColumn("unit_price",
            when(col("unit_price") === "abc" || col("unit_price").isNull, lit(0.0))
              .otherwise(col("unit_price").cast(DoubleType))
          )

        println(s"🧹 [SILVER STEP 3] Standardizing order status and store locations...")
        cleanedDf = cleanedDf
          .withColumn("order_status", initcap(trim(col("order_status"))))
          .withColumn("payment_method", initcap(trim(col("payment_method"))))

        println(s"🧹 [SILVER STEP 4] Correcting order anomalies...")
        cleanedDf = cleanedDf
          .withColumn("quantity", abs(col("quantity")))
      }

      println(s"📝 [SILVER STEP 5] Appending silver transformation timestamp lineage...")
      val finalDf = cleanedDf.withColumn("_silver_processed_timestamp", current_timestamp())

      println(s"💾 [SILVER STEP 6] Writing cleansed Silver dataset to: $silverOutputPath")
      finalDf.write
        .mode("overwrite")
        .parquet(silverOutputPath)

      println(s"✅ [SILVER COMPLETE] Successfully wrote cleansed dataset for $targetFileName to Silver layer.")

    } catch {
      case e: Exception =>
        allSuccessful = false
        println(s"❌ [SILVER ERROR] Failed processing silver pipeline for $targetFileName: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println(s"🛑 [SPARK SILVER] SparkSession stopped for file ID: $fileId")
    }

    if (allSuccessful) {
      publishEvent("retail.silver.completed.exchange", "silver.completed", fileId, targetFileName, "wasbs://files-artifact-container@filestorageaccount1997.blob.core.windows.net/", "SILVER")
    }
  }

  def runGoldPipeline(fileId: Long, targetFileName: String): Unit = {
    println(s"⚙️ [SPARK GOLD] Initializing Gold Aggregation SparkSession for File ID: $fileId, Target File: $targetFileName")

    val spark = SparkSession.builder()
      .appName(s"Retail-Gold-Aggregation-Pipeline-$fileId")
      .master("local[*]")
      .getOrCreate()

    var allSuccessful = true
    val goldOutputPath = s"./gold_output/retail_summary_mart"

    try {
      // STEP 1: Read cleansed data from Silver Parquet partitions/folders
      println(s"🔍 [GOLD STEP 1] Reading Silver datasets from disk...")
      val customersDf = spark.read.parquet("./silver_output/customers/*/*")
      val ordersDf = spark.read.parquet("./silver_output/orders/*/*")

      // STEP 2: Join customers and orders on customer_id
      println(s"🔗 [GOLD STEP 2] Joining customers and orders datasets...")
      val enrichedOrders = ordersDf.join(customersDf, "customer_id")

      // STEP 3: Compute business metrics and summaries
      println(s"📊 [GOLD STEP 3] Calculating business metrics (Total Spend, Order Counts)...")
      val customerSummaryDf = enrichedOrders
        .groupBy("customer_id", "customer_name", "city", "state")
        .agg(
          count("order_id").as("total_orders"),
          sum("unit_price").as("total_spend"),
          avg("unit_price").as("avg_order_value")
        )
        .withColumn("_gold_processed_timestamp", current_timestamp())

      // STEP 4: Write Gold Analytical Mart Parquet files
      println(s"💾 [GOLD STEP 4] Writing Gold analytical mart to: $goldOutputPath")
      customerSummaryDf.write
        .mode("overwrite")
        .parquet(goldOutputPath)

      println(s"✅ [GOLD COMPLETE] Successfully generated retail analytical gold mart.")

    } catch {
      case e: Exception =>
        allSuccessful = false
        println(s"❌ [GOLD ERROR] Failed processing gold pipeline: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println(s"🛑 [SPARK GOLD] SparkSession stopped for file ID: $fileId")
    }

    // STEP 5: Publish Gold Completion Event back to Orchestrator
    if (allSuccessful) {
      publishEvent("retail.gold.completed.exchange", "gold.completed", fileId, targetFileName, "wasbs://files-artifact-container@filestorageaccount1997.blob.core.windows.net/", "GOLD")
    }
  }

  def publishEvent(exchange: String, routingKey: String, fileId: Long, targetFileName: String, baseUrl: String, stage: String): Unit = {
    try {
      val factory = new ConnectionFactory()
      factory.setHost("localhost")
      factory.setPort(5672)
      factory.setUsername("guest")
      factory.setPassword("guest")

      val connection = factory.newConnection()
      val channel = connection.createChannel()

      channel.exchangeDeclare(exchange, "topic", true)

      val eventMessage =
        s"""{
           |  "id": $fileId,
           |  "fileName": "$targetFileName",
           |  "blobUrl": "$baseUrl",
           |  "currentStage": "$stage",
           |  "timestamp": "${LocalDateTime.now()}"
           |}""".stripMargin

      channel.basicPublish(exchange, routingKey, null, eventMessage.getBytes(StandardCharsets.UTF_8))
      println(s"📤 [RABBITMQ] Published $stage completion event for file ID: $fileId to exchange '$exchange'")

      channel.close()
      connection.close()
    } catch {
      case e: Exception =>
        println(s"⚠️ [RABBITMQ WARNING] Failed to publish $stage completion event: ${e.getMessage}")
    }
  }
}