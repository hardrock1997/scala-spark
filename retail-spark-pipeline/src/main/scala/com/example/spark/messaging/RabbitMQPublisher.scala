package com.example.spark.messaging

import com.rabbitmq.client.ConnectionFactory
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime

object RabbitMQPublisher {
  private val HOST = "localhost"
  private val PORT = 5672
  private val USER = "guest"
  private val PASS = "guest"

  def publishEvent(exchange: String, routingKey: String, fileId: Long, targetFileName: String, baseUrl: String, stage: String): Unit = {
    val factory = new ConnectionFactory()
    factory.setHost(HOST)
    factory.setPort(PORT)
    factory.setUsername(USER)
    factory.setPassword(PASS)

    var connection: com.rabbitmq.client.Connection = null
    var channel: com.rabbitmq.client.Channel = null

    try {
      connection = factory.newConnection()
      channel = connection.createChannel()
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
    } catch {
      case e: Exception =>
        println(s"⚠️ [RABBITMQ WARNING] Failed to publish $stage completion event: ${e.getMessage}")
    } finally {
      if (channel != null && channel.isOpen) channel.close()
      if (connection != null && connection.isOpen) connection.close()
    }
  }
}