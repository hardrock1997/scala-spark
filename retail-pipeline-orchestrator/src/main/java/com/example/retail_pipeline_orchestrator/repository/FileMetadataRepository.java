package com.example.retail_pipeline_orchestrator.repository;

import com.example.retail_pipeline_orchestrator.entity.FileMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface FileMetadataRepository extends JpaRepository<FileMetadata, Long> {
    @Modifying
    @Transactional
    @Query("UPDATE FileMetadata f SET f.status = :status WHERE f.id = :id")
    void updateStatus(@Param("id") Long id, @Param("status") String status);
}
