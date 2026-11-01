package com.example.retail_pipeline_orchestrator.service;

import com.example.retail_pipeline_orchestrator.events.FileIngestedEvent;
import com.example.retail_pipeline_orchestrator.repository.FileMetadataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;

@Service
@Slf4j
@RequiredArgsConstructor
public class PipelineOrchestratorService {

    private final FileMetadataRepository metadataRepository;

    public void updateFileStatus(Long id, String status) {
        metadataRepository.updateStatus(id, status);
        log.info("Updated file ID {} status to {}", id, status);
    }
}
