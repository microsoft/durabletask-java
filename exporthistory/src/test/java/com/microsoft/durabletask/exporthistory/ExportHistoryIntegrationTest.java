// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.DurableTaskGrpcWorker;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationFactory;
import com.microsoft.durabletask.TaskActivityFactory;
import com.microsoft.durabletask.TaskActivity;
import com.microsoft.durabletask.azureblobpayloads.BlobPayloadStore;
import com.microsoft.durabletask.azureblobpayloads.LargePayloadClientExtensions;
import com.microsoft.durabletask.azureblobpayloads.LargePayloadStorageOptions;
import com.microsoft.durabletask.azureblobpayloads.LargePayloadWorkerExtensions;
import com.microsoft.durabletask.azureblobpayloads.PayloadStore;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerClientOptions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerOptions;

import io.grpc.Channel;
import io.grpc.ManagedChannel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the export history feature.
 * <p>
 * These tests require:
 * <ul>
 *   <li>DTS emulator (>= v0.4.22 for ListInstanceIds) on localhost:4001:
 *       {@code docker run --name durabletask-emulator -p 4001:8080 -d mcr.microsoft.com/dts/dts-emulator:latest}</li>
 *   <li>Azurite on localhost:10000:
 *       {@code docker run --name azurite -p 10000:10000 -d mcr.microsoft.com/azure-storage/azurite}</li>
 * </ul>
 */
@Tag("integration")
public class ExportHistoryIntegrationTest {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final String AZURITE_CONNECTION_STRING =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        + "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    private static final String EMULATOR_ENDPOINT =
        System.getenv("DTS_ENDPOINT") != null ? System.getenv("DTS_ENDPOINT") : "http://localhost:4001";

    private static final String ECHO_ORCHESTRATION = "ExportHistoryEcho";

    private DurableTaskGrpcWorker worker;
    private DurableTaskClient client;
    private ManagedChannel workerChannel;
    private ManagedChannel clientChannel;

    @AfterEach
    void tearDown() {
        if (worker != null) {
            worker.stop();
            worker = null;
        }
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                // ignore
            }
            client = null;
        }
        if (workerChannel != null) {
            workerChannel.shutdownNow();
            workerChannel = null;
        }
        if (clientChannel != null) {
            clientChannel.shutdownNow();
            clientChannel = null;
        }
    }

    @Test
    void batchExport_writesOneBlobPerCompletedInstance() throws TimeoutException, InterruptedException {
        int instanceCount = 3;
        String container = "exporthistory-it-" + System.currentTimeMillis();

        ExportHistoryStorageOptions storage = new ExportHistoryStorageOptions()
                .setConnectionString(AZURITE_CONNECTION_STRING)
                .setContainerName(container);

        this.client = createClientBuilder().build();

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return ECHO_ORCHESTRATION;
            }

            @Override
            public TaskOrchestration create() {
                return ctx -> ctx.complete(ctx.getInput(String.class));
            }
        });
        ExportHistoryWorkerExtensions.useExportHistory(workerBuilder, storage, this.client);
        this.worker = workerBuilder.build();
        this.worker.start();

        Instant windowStart = Instant.now().minusSeconds(60);

        // Schedule and complete some orchestrations to export.
        List<String> instanceIds = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++) {
            String id = this.client.scheduleNewOrchestrationInstance(ECHO_ORCHESTRATION, "payload-" + i);
            instanceIds.add(id);
        }
        for (String id : instanceIds) {
            OrchestrationMetadata md = this.client.waitForInstanceCompletion(id, DEFAULT_TIMEOUT, false);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, md.getRuntimeStatus());
        }

        Instant windowEnd = Instant.now();

        // Create the export job.
        ExportHistoryClient export = ExportHistoryClientExtensions.useExportHistory(this.client, storage);
        ExportHistoryJobClient jobClient = export.createJob(new ExportJobCreationOptions("it-job-" + System.currentTimeMillis())
                .setMode(ExportMode.BATCH)
                .setCompletedTimeFrom(windowStart)
                .setCompletedTimeTo(windowEnd)
                .setMaxInstancesPerBatch(10));

        // Wait for the job to complete.
        ExportJobDescription description = waitForJobCompletion(jobClient, Duration.ofSeconds(60));
        assertNotNull(description);
        assertEquals(ExportJobStatus.COMPLETED, description.getStatus());
        assertTrue(description.getExportedInstances() >= instanceCount,
                "Expected at least " + instanceCount + " exported instances, got " + description.getExportedInstances());

        // Verify the blobs were written.
        long blobCount = countBlobs(container);
        assertTrue(blobCount >= instanceCount,
                "Expected at least " + instanceCount + " blobs in container " + container + ", found " + blobCount);
    }

    @Test
    void blobWriter_repeatedUploadAndContainerDeletionPreserveContentAndProperties() throws IOException {
        String container = "exporthistory-writer-it-" + System.currentTimeMillis();
        BlobServiceClient serviceClient = new BlobServiceClientBuilder()
                .connectionString(AZURITE_CONNECTION_STRING)
                .buildClient();
        BlobContainerClient containerClient = serviceClient.getBlobContainerClient(container);

        try {
            ExportHistoryStorageOptions storage = new ExportHistoryStorageOptions()
                    .setConnectionString(AZURITE_CONNECTION_STRING)
                    .setContainerName(container);
            BlobExportWriter writer = new BlobExportWriter(storage);
            ExportFormat format = new ExportFormat(ExportFormatKind.JSONL, "1.0");
            String blobPath = "history.jsonl.gz";

            writer.upload(container, blobPath, "first", format, "instance-1");
            writer.upload(container, blobPath, "second", format, "instance-2");
            assertEquals("second", readGzip(containerClient.getBlobClient(blobPath)));

            containerClient.delete();
            writer.upload(container, blobPath, "after-deletion", format, "instance-3");

            BlobClient blobClient = containerClient.getBlobClient(blobPath);
            BlobProperties properties = blobClient.getProperties();
            assertEquals("application/jsonl+gzip", properties.getContentType());
            assertEquals("gzip", properties.getContentEncoding());
            assertEquals("instance-3", properties.getMetadata().get("instanceId"));
            assertEquals("after-deletion", readGzip(blobClient));
        } finally {
            containerClient.deleteIfExists();
        }
    }

    @Test
    void batchExport_largePayloadHistoryContainsResolvedInputAndOutput() throws Exception {
        String container = "exporthistory-large-" + UUID.randomUUID();
        String payloadContainer = "exporthistory-payload-" + UUID.randomUUID();
        String payload = "abcdef".repeat(250_000);
        ExportHistoryStorageOptions storage = storageOptions(container);
        LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
                .setConnectionString(AZURITE_CONNECTION_STRING)
                .setContainerName(payloadContainer);
        PayloadStore store = new BlobPayloadStore(payloadOptions);
        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        this.client = clientBuilder.build();
        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);
        addEchoOrchestration(workerBuilder);
        ExportHistoryWorkerExtensions.useExportHistory(workerBuilder, storage, this.client);
        this.worker = workerBuilder.build();
        this.worker.start();

        try {
            Instant from = Instant.now();
            String id = this.client.scheduleNewOrchestrationInstance(ECHO_ORCHESTRATION, payload);
            OrchestrationMetadata result = this.client.waitForInstanceCompletion(id, DEFAULT_TIMEOUT, true);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, result.getRuntimeStatus());
            assertEquals(payload, result.readOutputAs(String.class));
            BlobContainerClient payloadBlobs = containerClient(payloadContainer);
            assertTrue(payloadBlobs.listBlobs().stream().count() >= 2,
                    "Input and output must actually be externalized to Blob Storage");

            exportBatch(storage, from, ExportFormatKind.JSON);
            JsonNode events = new ObjectMapper().readTree(
                    readExport(findExportedBlob(container, id), ExportFormatKind.JSON));
            ObjectMapper mapper = new ObjectMapper();
            assertEquals(payload, mapper.readTree(findEvent(events, "ExecutionStarted").path("input").asText())
                    .asText());
            assertEquals(payload, mapper.readTree(findEvent(events, "ExecutionCompleted").path("result").asText())
                    .asText());
        } finally {
            containerClient(container).deleteIfExists();
            containerClient(payloadContainer).deleteIfExists();
        }
    }

    @Test
    void batchExport_failedActivityPreservesPropertiesInBothFormats() throws Exception {
        String container = "exporthistory-failure-" + UUID.randomUUID();
        ExportHistoryStorageOptions storage = storageOptions(container);
        this.client = createClientBuilder().build();
        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder()
                .exceptionPropertiesProvider(ex -> ex instanceof IllegalStateException
                        ? Collections.singletonMap("diagnosticCode", "e2e-failure") : Collections.emptyMap());
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ExportFailure"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> ctx.callActivity("ExportFailureActivity", null, String.class).await();
            }
        });
        workerBuilder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "ExportFailureActivity"; }

            @Override
            public TaskActivity create() {
                return ctx -> { throw new IllegalStateException("e2e activity failure"); };
            }
        });
        ExportHistoryWorkerExtensions.useExportHistory(workerBuilder, storage, this.client);
        this.worker = workerBuilder.build();
        this.worker.start();
        try {
            Instant from = Instant.now();
            String id = this.client.scheduleNewOrchestrationInstance("ExportFailure");
            assertEquals(OrchestrationRuntimeStatus.FAILED,
                    this.client.waitForInstanceCompletion(id, DEFAULT_TIMEOUT, false).getRuntimeStatus());
            ObjectMapper mapper = new ObjectMapper();
            for (ExportFormatKind kind : ExportFormatKind.values()) {
                containerClient(container).deleteIfExists();
                exportBatch(storage, from, kind);
                String content = readExport(findExportedBlob(container, id), kind);
                JsonNode events = mapper.readTree(kind == ExportFormatKind.JSON
                        ? content : "[" + content.trim().replace("\n", ",") + "]");
                JsonNode failure = findEvent(events, "TaskFailed").path("failureDetails");
                assertEquals("e2e activity failure", failure.path("errorMessage").asText());
                assertEquals("e2e-failure", failure.path("properties").path("diagnosticCode").asText());
                assertEquals(mapper.createObjectNode(),
                        findEvent(events, "ExecutionCompleted").path("failureDetails").path("properties"));
            }
        } finally {
            containerClient(container).deleteIfExists();
        }
    }

    @Test
    void continuousExport_includesCompletionBetweenOptionsConstructionAndJobCreation() throws Exception {
        String container = "exporthistory-continuous-" + UUID.randomUUID();
        ExportHistoryStorageOptions storage = storageOptions(container);
        this.client = createClientBuilder().build();
        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        addEchoOrchestration(workerBuilder);
        ExportHistoryWorkerExtensions.useExportHistory(workerBuilder, storage, this.client);
        this.worker = workerBuilder.build();
        this.worker.start();
        ExportHistoryJobClient job = null;
        try {
            ExportJobCreationOptions options = new ExportJobCreationOptions("continuous-" + UUID.randomUUID())
                    .setMode(ExportMode.CONTINUOUS);
            Instant submittedFrom = options.getCompletedTimeFrom();
            String id = this.client.scheduleNewOrchestrationInstance(ECHO_ORCHESTRATION, "before-job-creation");
            OrchestrationMetadata completed = this.client.waitForInstanceCompletion(id, DEFAULT_TIMEOUT, false);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, completed.getRuntimeStatus());
            assertTrue(completed.getLastUpdatedAt().isAfter(submittedFrom));
            job = ExportHistoryClientExtensions.useExportHistory(this.client, storage).createJob(options);
            ExportJobDescription description = job.describe();
            Instant persistedFrom = description.getConfig().getFilter().getCompletedTimeFrom();
            assertTrue(Duration.between(submittedFrom, persistedFrom).abs().compareTo(Duration.ofNanos(1000)) <= 0,
                    "Entity-state transport must preserve the construction-time bound within one microsecond");
            assertTrue(description.getCreatedAt().isAfter(completed.getLastUpdatedAt()));
            Instant deadline = Instant.now().plusSeconds(60);
            BlobClient exported = null;
            do {
                BlobContainerClient blobs = containerClient(container);
                if (blobs.exists()) {
                    exported = blobs.listBlobs().stream()
                            .filter(blob -> id.equals(blobs.getBlobClient(blob.getName())
                                    .getProperties().getMetadata().get("instanceId")))
                            .map(blob -> blobs.getBlobClient(blob.getName())).findFirst().orElse(null);
                }
                if (exported == null) {
                    Thread.sleep(500);
                }
            } while (exported == null && Instant.now().isBefore(deadline));
            assertNotNull(exported, "Continuous export must include the completion before job creation");
            assertTrue(readGzip(exported).contains("before-job-creation"));
        } finally {
            if (job != null) {
                job.delete();
            }
            containerClient(container).deleteIfExists();
        }
    }

    private void exportBatch(ExportHistoryStorageOptions storage, Instant from, ExportFormatKind kind)
            throws InterruptedException {
        ExportHistoryJobClient job = ExportHistoryClientExtensions.useExportHistory(this.client, storage)
                .createJob(new ExportJobCreationOptions("batch-" + UUID.randomUUID())
                        .setMode(ExportMode.BATCH)
                        .setCompletedTimeFrom(from)
                        .setCompletedTimeTo(Instant.now())
                        .setFormat(new ExportFormat(kind, "1.0")));
        ExportJobDescription result = waitForJobCompletion(job, Duration.ofSeconds(60));
        assertEquals(ExportJobStatus.COMPLETED, result.getStatus(), result.getLastError());
    }

    private static ExportHistoryStorageOptions storageOptions(String container) {
        return new ExportHistoryStorageOptions().setConnectionString(AZURITE_CONNECTION_STRING)
                .setContainerName(container);
    }

    private static BlobContainerClient containerClient(String container) {
        return new BlobServiceClientBuilder().connectionString(AZURITE_CONNECTION_STRING).buildClient()
                .getBlobContainerClient(container);
    }

    private static BlobClient findExportedBlob(String container, String instanceId) {
        BlobContainerClient blobs = containerClient(container);
        return blobs.listBlobs().stream().filter(blob -> instanceId.equals(blobs.getBlobClient(blob.getName())
                        .getProperties().getMetadata().get("instanceId")))
                .map(blob -> blobs.getBlobClient(blob.getName())).findFirst()
                .orElseThrow(() -> new AssertionError("Missing exported history for " + instanceId));
    }

    private static String readGzip(BlobClient blob) throws IOException {
        try (GZIPInputStream stream = new GZIPInputStream(
                new ByteArrayInputStream(blob.downloadContent().toBytes()))) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String readExport(BlobClient blob, ExportFormatKind kind) throws IOException {
        BlobProperties properties = blob.getProperties();
        if (kind == ExportFormatKind.JSON) {
            assertEquals("application/json", properties.getContentType());
            assertTrue(properties.getContentEncoding() == null || properties.getContentEncoding().isEmpty());
            return new String(blob.downloadContent().toBytes(), StandardCharsets.UTF_8);
        }
        assertEquals("application/jsonl+gzip", properties.getContentType());
        assertEquals("gzip", properties.getContentEncoding());
        return readGzip(blob);
    }

    private static JsonNode findEvent(JsonNode events, String eventType) {
        for (JsonNode event : events) {
            if (eventType.equals(event.path("eventType").asText())) {
                return event;
            }
        }
        throw new AssertionError("Missing event " + eventType);
    }

    private static void addEchoOrchestration(DurableTaskGrpcWorkerBuilder builder) {
        builder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return ECHO_ORCHESTRATION; }

            @Override
            public TaskOrchestration create() {
                return ctx -> ctx.complete(ctx.getInput(String.class));
            }
        });
    }

    private ExportJobDescription waitForJobCompletion(ExportHistoryJobClient jobClient, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        ExportJobDescription description = jobClient.describe();
        while (Instant.now().isBefore(deadline)) {
            description = jobClient.describe();
            if (description.getStatus() == ExportJobStatus.COMPLETED
                    || description.getStatus() == ExportJobStatus.FAILED) {
                return description;
            }
            Thread.sleep(2000);
        }
        return description;
    }

    private static long countBlobs(String container) {
        BlobServiceClient serviceClient = new BlobServiceClientBuilder()
                .connectionString(AZURITE_CONNECTION_STRING)
                .buildClient();
        BlobContainerClient containerClient = serviceClient.getBlobContainerClient(container);
        if (!containerClient.exists()) {
            return 0;
        }
        return containerClient.listBlobs().stream().count();
    }

    private DurableTaskGrpcWorkerBuilder createWorkerBuilder() {
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions()
                .setEndpointAddress(EMULATOR_ENDPOINT)
                .setTaskHubName("default")
                .setCredential(null)
                .setAllowInsecureCredentials(true);
        Channel grpcChannel = options.createGrpcChannel();
        this.workerChannel = (ManagedChannel) grpcChannel;
        return new DurableTaskGrpcWorkerBuilder().grpcChannel(grpcChannel);
    }

    private DurableTaskGrpcClientBuilder createClientBuilder() {
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions()
                .setEndpointAddress(EMULATOR_ENDPOINT)
                .setTaskHubName("default")
                .setCredential(null)
                .setAllowInsecureCredentials(true);
        Channel grpcChannel = options.createGrpcChannel();
        this.clientChannel = (ManagedChannel) grpcChannel;
        return new DurableTaskGrpcClientBuilder().grpcChannel(grpcChannel);
    }
}
