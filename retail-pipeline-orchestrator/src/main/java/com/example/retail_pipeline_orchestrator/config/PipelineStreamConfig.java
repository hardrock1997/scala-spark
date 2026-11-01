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

                // 🚀 Automatically launch Spark via ProcessBuilder, passing ID and Filename
                String sparkArg = event.getId() + "," + event.getFileName();
                log.info("🚀 [ORCHESTRATOR] Launching Spark Bronze Pipeline for: {}", sparkArg);

                ProcessBuilder processBuilder = new ProcessBuilder("./gradlew", "run", "--args=" + sparkArg);
                processBuilder.inheritIO(); // Pipes Spark logs directly into your Spring Boot console
                processBuilder.start();

            } catch (Exception e) {
                log.error("❌ [ERROR] Failed to launch Spark for ID: {}. Reason: {}", event.getId(), e.getMessage(), e);
            }
        };
    }

    @Bean
    public Consumer<PipelineStageEvent> bronzeCompletedConsumer() {
        return event -> {
            try {
                log.info("✅ [CONSUMER] bronzeCompletedConsumer triggered. ID: {}, CurrentStage: {}", event.getId(), event.getCurrentStage());
                orchestratorService.updateFileStatus(event.getId(), "SILVER_PROCESSING");
                // TODO: Trigger Silver Layer processing here
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
            } catch (Exception e) {
                log.error("❌ [ERROR] Failed silverCompletedConsumer: {}", e.getMessage(), e);
            }
        };
    }
}