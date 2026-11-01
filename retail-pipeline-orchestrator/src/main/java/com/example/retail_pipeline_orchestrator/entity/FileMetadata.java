package com.example.retail_pipeline_orchestrator.entity;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Table(name = "file_metadata")
@Data
public class FileMetadata {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "file_name")
    private String fileName;

    @Column(name = "blob_url")
    private String blobUrl;

    private String status;

    @Column(name = "uploaded_at")
    private LocalDateTime uploadedAt;
}
