package com.example.retail_pipeline_orchestrator.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PipelineStageEvent {
    private Long id;
    private String fileName;
    private String blobUrl;
    private String currentStage; // e.g., "BRONZE", "SILVER", "GOLD"
    private LocalDateTime timestamp;
}
