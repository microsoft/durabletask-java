// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.activities;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.OrchestrationHistoryEvent;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.TaskActivity;
import com.microsoft.durabletask.TaskActivityContext;
import com.microsoft.durabletask.exporthistory.models.ExportFormatKind;
import com.microsoft.durabletask.exporthistory.models.ExportRequest;
import com.microsoft.durabletask.exporthistory.models.ExportResult;
import com.microsoft.durabletask.exporthistory.options.ExportHistoryStorageOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * Activity that exports one orchestration instance's history to Azure Blob Storage.
 */
public class ExportInstanceHistoryActivity implements TaskActivity {

    private static final Logger logger = Logger.getLogger(ExportInstanceHistoryActivity.class.getName());

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setPropertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private final DurableTaskClient client;
    private final ExportHistoryStorageOptions storageOptions;

    // Lazily initialized and cached to avoid rebuilding per invocation
    private volatile BlobContainerClient cachedContainerClient;
    private volatile String cachedContainerName;

    public ExportInstanceHistoryActivity(DurableTaskClient client, ExportHistoryStorageOptions storageOptions) {
        this.client = client;
        this.storageOptions = storageOptions;
    }

    @Override
    public Object run(TaskActivityContext ctx) {
        ExportRequest input = ctx.getInput(ExportRequest.class);
        if (input == null || input.getInstanceId() == null || input.getInstanceId().isEmpty()) {
            throw new IllegalArgumentException("ExportRequest with instanceId is required.");
        }

        String instanceId = input.getInstanceId();

        try {
            logger.info("Starting export for instance " + instanceId);

            // Get instance metadata
            OrchestrationMetadata metadata = this.client.getInstanceMetadata(instanceId, true);
            if (metadata == null || !metadata.isInstanceFound()) {
                return new ExportResult(instanceId, false, "Instance not found.");
            }
            if (!metadata.isCompleted()) {
                return new ExportResult(instanceId, false, "Instance is not in a completed state.");
            }

            Instant completedTimestamp = metadata.getLastUpdatedAt();

            // Stream history events
            List<OrchestrationHistoryEvent> historyEvents = this.client.getOrchestrationHistory(instanceId);
            logger.info("Retrieved " + historyEvents.size() + " history events for instance " + instanceId);

            // Generate blob name
            String blobFileName = generateBlobFileName(completedTimestamp, instanceId, input.getFormat().getKind());
            String blobPath = input.getDestination().getPrefix() != null
                    ? input.getDestination().getPrefix().replaceAll("/$", "") + "/" + blobFileName
                    : blobFileName;

            // Serialize
            String jsonContent = serializeEvents(historyEvents, input.getFormat().getKind());

            // Upload
            uploadToBlob(
                    input.getDestination().getContainer(),
                    blobPath,
                    jsonContent,
                    input.getFormat().getKind(),
                    instanceId);

            logger.info("Successfully exported instance " + instanceId + " to " + blobPath);
            return new ExportResult(instanceId, true, null);

        } catch (RuntimeException ex) {
            // Let transient failures (blob upload, serialization, network) propagate
            // so the activity-level retry policy in the orchestrator can apply.
            logger.log(Level.SEVERE, "Failed to export instance " + instanceId, ex);
            throw ex;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Failed to export instance " + instanceId, ex);
            throw new RuntimeException(
                    "Export failed for instance " + instanceId + ": " + ex.getMessage(), ex);
        }
    }

    private static String generateBlobFileName(Instant completedTimestamp, String instanceId, ExportFormatKind kind) {
        try {
            String hashInput = completedTimestamp.toString() + "|" + instanceId;
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(hashInput.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hashBytes) {
                hex.append(String.format("%02x", b));
            }
            String extension = kind == ExportFormatKind.JSONL ? "jsonl.gz" : "json";
            return hex.toString() + "." + extension;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private static String serializeEvents(List<OrchestrationHistoryEvent> events, ExportFormatKind kind) {
        try {
            if (kind == ExportFormatKind.JSONL) {
                StringBuilder sb = new StringBuilder();
                for (OrchestrationHistoryEvent event : events) {
                    sb.append(MAPPER.writeValueAsString(event)).append('\n');
                }
                return sb.toString();
            } else {
                return MAPPER.writeValueAsString(events);
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize history events", e);
        }
    }

    private void uploadToBlob(
            String containerName,
            String blobPath,
            String content,
            ExportFormatKind kind,
            String instanceId) throws IOException {

        BlobContainerClient containerClient = getOrCreateContainerClient(containerName);

        BlobClient blobClient = containerClient.getBlobClient(blobPath);
        Map<String, String> metadata = Collections.singletonMap("instanceId", instanceId);

        if (kind == ExportFormatKind.JSONL) {
            // Gzip compress
            byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
                gzip.write(contentBytes);
            }
            byte[] compressed = baos.toByteArray();

            BlobParallelUploadOptions uploadOptions = new BlobParallelUploadOptions(
                    new ByteArrayInputStream(compressed), compressed.length)
                    .setHeaders(new BlobHttpHeaders()
                            .setContentType("application/jsonl+gzip")
                            .setContentEncoding("gzip"))
                    .setMetadata(metadata);
            blobClient.uploadWithResponse(uploadOptions, null, null);
        } else {
            byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
            BlobParallelUploadOptions uploadOptions = new BlobParallelUploadOptions(
                    new ByteArrayInputStream(contentBytes), contentBytes.length)
                    .setHeaders(new BlobHttpHeaders()
                            .setContentType("application/json"))
                    .setMetadata(metadata);
            blobClient.uploadWithResponse(uploadOptions, null, null);
        }
    }

    private BlobContainerClient getOrCreateContainerClient(String containerName) {
        // Double-checked locking: reuse the cached client if the container name matches
        if (this.cachedContainerClient != null && containerName.equals(this.cachedContainerName)) {
            return this.cachedContainerClient;
        }
        synchronized (this) {
            if (this.cachedContainerClient != null && containerName.equals(this.cachedContainerName)) {
                return this.cachedContainerClient;
            }
            BlobServiceClient serviceClient = new BlobServiceClientBuilder()
                    .connectionString(this.storageOptions.getConnectionString())
                    .buildClient();
            BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);
            containerClient.createIfNotExists();
            this.cachedContainerName = containerName;
            this.cachedContainerClient = containerClient;
            return containerClient;
        }
    }
}
