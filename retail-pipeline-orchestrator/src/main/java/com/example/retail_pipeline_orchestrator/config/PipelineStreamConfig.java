package com.example.retail_pipeline_orchestrator.config;

import com.example.retail_pipeline_orchestrator.events.FileIngestedEvent;
import com.example.retail_pipeline_orchestrator.events.PipelineStageEvent;
import com.example.retail_pipeline_orchestrator.service.PipelineOrchestratorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;
import java.util.function.Consumer;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class PipelineStreamConfig {
    private final StreamBridge streamBridge;
    private final PipelineOrchestratorService orchestratorService;

    @Bean
    public Consumer<FileIngestedEvent> fileIngestedConsumer() {
        return event -> {
            try {
                log.info("🎯 [CONSUMER] fileIngestedConsumer triggered. Payload received -> ID: {}, FileName: {}",
                        event.getId(), event.getFileName());

                orchestratorService.updateFileStatus(event.getId(), "BRONZE_PROCESSING");

                PipelineStageEvent stageEvent = PipelineStageEvent.builder()
                        .id(event.getId())
                        .fileName(event.getFileName())
                        .blobUrl(event.getBlobUrl())
                        .currentStage("BRONZE_PROCESSING")
                        .timestamp(LocalDateTime.now())
                        .build();

                log.info("🚀 [ORCHESTRATOR] Publishing trigger to bronzeTriggerOut-out-0 for ID: {}", event.getId());
                streamBridge.send("bronzeTriggerOut-out-0", stageEvent);

            } catch (Exception e) {
                log.error("❌ [ERROR] Failed in fileIngestedConsumer for ID: {}. Reason: {}", event.getId(), e.getMessage(), e);
            }
        };
    }

    @Bean
    public Consumer<PipelineStageEvent> bronzeCompletedConsumer() {
        return event -> {
            try {
                log.info("✅ [CONSUMER] bronzeCompletedConsumer triggered. ID: {}, CurrentStage: {}", event.getId(), event.getCurrentStage());

                orchestratorService.updateFileStatus(event.getId(), "SILVER_PROCESSING");

                PipelineStageEvent stageEvent = PipelineStageEvent.builder()
                        .id(event.getId())
                        .fileName(event.getFileName())
                        .blobUrl(event.getBlobUrl())
                        .currentStage("SILVER_PROCESSING")
                        .timestamp(LocalDateTime.now())
                        .build();

                log.info("🥈 [ORCHESTRATOR] Publishing trigger to silverTriggerOut-out-0 for ID: {}", event.getId());
                streamBridge.send("silverTriggerOut-out-0", stageEvent);

            } catch (Exception e) {
                log.error("❌ [ERROR] Failed bronzeCompletedConsumer: {}", e.getMessage(), e);
            }
        };
    }

    @Bean
    public Consumer<PipelineStageEvent> silverCompletedConsumer() {
        return event -> {
            try {
                log.info("✅ [CONSUMER] silverCompletedConsumer triggered. ID: {}", event.getId());

                orchestratorService.updateFileStatus(event.getId(), "GOLD_PROCESSING");

                PipelineStageEvent stageEvent = PipelineStageEvent.builder()
                        .id(event.getId())
                        .fileName(event.getFileName())
                        .blobUrl(event.getBlobUrl())
                        .currentStage("GOLD_PROCESSING")
                        .timestamp(LocalDateTime.now())
                        .build();

                log.info("🥇 [ORCHESTRATOR] Publishing trigger to goldTriggerOut-out-0 for ID: {}", event.getId());
                streamBridge.send("goldTriggerOut-out-0", stageEvent);

            } catch (Exception e) {
                log.error("❌ [ERROR] Failed silverCompletedConsumer: {}", e.getMessage(), e);
            }
        };
    }

    @Bean
    public Consumer<PipelineStageEvent> goldCompletedConsumer() {
        return event -> {
            try {
                log.info("✅ [CONSUMER] goldCompletedConsumer triggered. ID: {}, CurrentStage: {}", event.getId(), event.getCurrentStage());

                // Finalize database status to COMPLETED
                orchestratorService.updateFileStatus(event.getId(), "COMPLETED");

                log.info("🎉 [ORCHESTRATOR] Pipeline successfully completed for File ID: {}", event.getId());

            } catch (Exception e) {
                log.error("❌ [ERROR] Failed goldCompletedConsumer for ID: {}. Reason: {}", event.getId(), e.getMessage(), e);
            }
        };
    }
}