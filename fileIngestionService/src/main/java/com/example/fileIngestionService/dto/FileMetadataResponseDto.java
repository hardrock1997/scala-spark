package com.example.fileIngestionService.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileMetadataResponseDto {
    private Long id;
    private String fileName;
    private String blobUrl;
    private String status; // PENDING, PROCESSED, FAILED
    private LocalDateTime uploadedAt;
}
