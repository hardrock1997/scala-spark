package com.example.spark.model

case class PipelineEvent(
                          id: Long,
                          fileName: String,
                          blobUrl: String,
                          currentStage: String,
                          timestamp: String
                        )