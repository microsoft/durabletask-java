// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.DurableTaskGrpcWorker;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationFactory;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerClientOptions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerOptions;

import io.grpc.Channel;
import io.grpc.ManagedChannel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

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
        long count = 0;
        for (BlobItem ignored : containerClient.listBlobs()) {
            count++;
        }
        return count;
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
