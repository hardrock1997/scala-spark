package com.example.fileIngestionService.controller;

import com.example.fileIngestionService.event.FileIngestedEvent;
import com.example.fileIngestionService.dto.FileMetadataResponseDto;
import com.example.fileIngestionService.entity.FileMetadata;
import com.example.fileIngestionService.repository.FileMetadataRepository;
import com.example.fileIngestionService.service.AzureStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/files")
@RequiredArgsConstructor
@Slf4j
public class FileController {
    private final AzureStorageService azureStorageService;
    private final FileMetadataRepository fileMetadataRepository;
    private final StreamBridge streamBridge;


    @PostMapping("/upload")
    public ResponseEntity<FileMetadataResponseDto> uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            // 1. Upload the raw file to Azure Blob Storage container
            String blobUrl = azureStorageService.uploadFile(file);

            // 2. Persist tracking metadata into PostgreSQL retail_db
            FileMetadata metadata = new FileMetadata();
            metadata.setFileName(file.getOriginalFilename());
            metadata.setBlobUrl(blobUrl);
            metadata.setStatus("PENDING");
            metadata.setUploadedAt(LocalDateTime.now());

            FileMetadata savedMetadata = fileMetadataRepository.save(metadata);

            FileIngestedEvent event = FileIngestedEvent.builder()
                    .id(savedMetadata.getId())
                    .fileName(savedMetadata.getFileName())
                    .blobUrl(savedMetadata.getBlobUrl())
                    .status(savedMetadata.getStatus())
                    .uploadedAt(savedMetadata.getUploadedAt())
                    .build();

            streamBridge.send("fileIngestionOut-out-0", event);

            // 3. Construct response DTO
            FileMetadataResponseDto responseDto = FileMetadataResponseDto.builder()
                    .id(savedMetadata.getId())
                    .fileName(savedMetadata.getFileName())
                    .blobUrl(savedMetadata.getBlobUrl())
                    .status(savedMetadata.getStatus())
                    .uploadedAt(savedMetadata.getUploadedAt())
                    .build();

            return ResponseEntity.ok(responseDto);

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/upload-batch")
    public ResponseEntity<List<FileMetadataResponseDto>> uploadBatchFiles(@RequestParam("files") List<MultipartFile> files) {
        List<FileMetadataResponseDto> responseDtos = new ArrayList<>();

        for (MultipartFile file : files) {
            try {
                // 1. Upload each file to Azure Blob Storage container
                String blobUrl = azureStorageService.uploadFile(file);

                // 2. Persist tracking metadata for each file into PostgreSQL
                FileMetadata metadata = new FileMetadata();
                metadata.setFileName(file.getOriginalFilename());
                metadata.setBlobUrl(blobUrl);
                metadata.setStatus("PENDING");
                metadata.setUploadedAt(LocalDateTime.now());

                FileMetadata savedMetadata = fileMetadataRepository.save(metadata);

                // 3. Publish individual event to Spring Cloud Stream / RabbitMQ
                FileIngestedEvent event = FileIngestedEvent.builder()
                        .id(savedMetadata.getId())
                        .fileName(savedMetadata.getFileName())
                        .blobUrl(savedMetadata.getBlobUrl())
                        .status(savedMetadata.getStatus())
                        .uploadedAt(savedMetadata.getUploadedAt())
                        .build();

                streamBridge.send("fileIngestionOut-out-0", event);

                log.info("📁 [BATCH INGESTION] Successfully processed file: ID={}, Name={}", savedMetadata.getId(), savedMetadata.getFileName());

                // 4. Map to DTO and collect
                FileMetadataResponseDto responseDto = FileMetadataResponseDto.builder()
                        .id(savedMetadata.getId())
                        .fileName(savedMetadata.getFileName())
                        .blobUrl(savedMetadata.getBlobUrl())
                        .status(savedMetadata.getStatus())
                        .uploadedAt(savedMetadata.getUploadedAt())
                        .build();

                responseDtos.add(responseDto);

            } catch (Exception e) {
                log.error("❌ [BATCH ERROR] Failed processing file {}: {}", file.getOriginalFilename(), e.getMessage());
            }
        }

        return ResponseEntity.ok(responseDtos);
    }
}
