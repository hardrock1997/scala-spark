package com.example.fileIngestionService.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.UUID;

@Service
public class AzureStorageService {
    private final BlobContainerClient blobContainerClient;


    public AzureStorageService(
            @Value("${spring.cloud.azure.storage.blob.connection-string}") String connectionString,
            @Value("${spring.cloud.azure.storage.blob.container-name}") String containerName) {

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();

        this.blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);

        if (!blobContainerClient.exists()) {
            blobContainerClient.create();
        }
    }

    public String uploadFile(MultipartFile file) throws IOException {
        String originalFilename = file.getOriginalFilename();
        // Generate a unique file name to prevent overwriting collisions
        String uniqueFileName = UUID.randomUUID() + "_" + (originalFilename != null ? originalFilename : "file");

        BlobClient blobClient = blobContainerClient.getBlobClient(uniqueFileName);
        blobClient.upload(file.getInputStream(), file.getSize(), true);

        return blobClient.getBlobUrl();
    }
}
