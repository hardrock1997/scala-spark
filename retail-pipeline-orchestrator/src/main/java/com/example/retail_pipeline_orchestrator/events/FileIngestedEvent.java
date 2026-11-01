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
public class FileIngestedEvent {
    private Long id;
    private String fileName;
    private String blobUrl;
    private String status;
    private LocalDateTime uploadedAt;
}
