package com.example.spark

import com.example.spark.model.PipelineEvent
import com.example.spark.service.PipelineExecutor
import com.rabbitmq.client._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.charset.StandardCharsets

object SparkPipelineConsumer {
  def main(args: Array[String]): Unit = {
    val RABBIT_HOST = "localhost"

    val EXCHANGE_BRONZE = "retail.bronze.trigger.exchange"
    val QUEUE_BRONZE = "retail.spark.bronze.trigger.queue"

    val EXCHANGE_SILVER = "retail.silver.trigger.exchange"
    val QUEUE_SILVER = "retail.spark.silver.trigger.queue"

    val EXCHANGE_GOLD = "retail.gold.trigger.exchange"
    val QUEUE_GOLD = "retail.spark.gold.trigger.queue"

    val factory = new ConnectionFactory()
    factory.setHost(RABBIT_HOST)

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    // Declare Bindings
    channel.exchangeDeclare(EXCHANGE_BRONZE, "topic", true)
    channel.queueDeclare(QUEUE_BRONZE, true, false, false, null)
    channel.queueBind(QUEUE_BRONZE, EXCHANGE_BRONZE, "#")

    channel.exchangeDeclare(EXCHANGE_SILVER, "topic", true)
    channel.queueDeclare(QUEUE_SILVER, true, false, false, null)
    channel.queueBind(QUEUE_SILVER, EXCHANGE_SILVER, "#")

    channel.exchangeDeclare(EXCHANGE_GOLD, "topic", true)
    channel.queueDeclare(QUEUE_GOLD, true, false, false, null)
    channel.queueBind(QUEUE_GOLD, EXCHANGE_GOLD, "#")

    println(s"[*] Modular Spark Pipeline Consumer daemon started.")

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val createCallback: (String, (Long, String) => Unit) => DeliverCallback = (stageName, executorFunc) => {
      (consumerTag: String, delivery: Delivery) => {
        val messageJson = new String(delivery.getBody, StandardCharsets.UTF_8)
        println(s"🎯 [$stageName CONSUMER] Received trigger: $messageJson")
        try {
          val event = mapper.readValue(messageJson, classOf[PipelineEvent])
          executorFunc(event.id, event.fileName)
          channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
          println(s"✅ [$stageName] Successfully processed file ID: ${event.id}")
        } catch {
          case e: Exception =>
            println(s"❌ [$stageName ERROR]: ${e.getMessage}")
            e.printStackTrace()
            channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
        }
      }
    }

    channel.basicConsume(QUEUE_BRONZE, false, createCallback("SPARK BRONZE", PipelineExecutor.executeBronze), (_: String) => {})
    channel.basicConsume(QUEUE_SILVER, false, createCallback("SPARK SILVER", PipelineExecutor.executeSilver), (_: String) => {})
    channel.basicConsume(QUEUE_GOLD, false, createCallback("SPARK GOLD", PipelineExecutor.executeGold), (_: String) => {})
  }
}