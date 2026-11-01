package com.example.fileIngestionService.controller;

import com.example.fileIngestionService.event.FileIngestedEvent;
import com.example.fileIngestionService.dto.FileMetadataResponseDto;
import com.example.fileIngestionService.entity.FileMetadata;
import com.example.fileIngestionService.repository.FileMetadataRepository;
import com.example.fileIngestionService.service.AzureStorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/files")
@RequiredArgsConstructor
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
}
