// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.DurableTaskGrpcWorker;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationFactory;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerClientExtensions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerExtensions;
import com.microsoft.durabletask.exporthistory.ExportHistoryClient;
import com.microsoft.durabletask.exporthistory.ExportHistoryClientExtensions;
import com.microsoft.durabletask.exporthistory.ExportHistoryJobClient;
import com.microsoft.durabletask.exporthistory.ExportHistoryStorageOptions;
import com.microsoft.durabletask.exporthistory.ExportHistoryWorkerExtensions;
import com.microsoft.durabletask.exporthistory.ExportJobCreationOptions;
import com.microsoft.durabletask.exporthistory.ExportJobDescription;
import com.microsoft.durabletask.exporthistory.ExportJobStatus;
import com.microsoft.durabletask.exporthistory.ExportMode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Demonstrates durable export of terminal orchestration history to Azure Blob Storage.
 *
 * <p>This sample schedules a few short orchestrations, waits for them to complete, then creates a BATCH export job
 * that archives their history (gzipped JSONL) to a blob container.
 *
 * <h3>Prerequisites</h3>
 * <ol>
 *   <li>DTS emulator: {@code docker run -d -p 8080:8080 mcr.microsoft.com/dts/dts-emulator:latest}</li>
 *   <li>Azurite: {@code docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0}</li>
 * </ol>
 *
 * <h3>Running</h3>
 * <pre>
 *   ./gradlew :samples:runHistoryExportSample
 * </pre>
 *
 * <h3>Environment variables</h3>
 * <ul>
 *   <li>{@code DURABLE_TASK_SCHEDULER_CONNECTION_STRING} — DTS connection string
 *       (default: {@code Endpoint=http://localhost:8080;TaskHub=default;Authentication=None})</li>
 *   <li>{@code EXPORT_HISTORY_STORAGE_CONNECTION_STRING} — Azure Storage connection string
 *       (default: {@code UseDevelopmentStorage=true})</li>
 *   <li>{@code EXPORT_HISTORY_CONTAINER} — blob container name (default: {@code orchestration-history})</li>
 * </ul>
 */
final class HistoryExportSample {

    private static final Logger logger = Logger.getLogger(HistoryExportSample.class.getName());
    private static final String ORCHESTRATION_NAME = "HistoryExportEcho";
    private static final int INSTANCE_COUNT = 3;

    private HistoryExportSample() {
    }

    public static void main(String[] args) throws InterruptedException, TimeoutException {
        String schedulerConnectionString = envOrDefault(
                "DURABLE_TASK_SCHEDULER_CONNECTION_STRING",
                "Endpoint=http://localhost:8080;TaskHub=default;Authentication=None");
        String storageConnectionString = envOrDefault(
                "EXPORT_HISTORY_STORAGE_CONNECTION_STRING",
                "UseDevelopmentStorage=true");
        String container = envOrDefault("EXPORT_HISTORY_CONTAINER", "orchestration-history");

        ExportHistoryStorageOptions storage = new ExportHistoryStorageOptions()
                .setConnectionString(storageConnectionString)
                .setContainerName(container)
                .setPrefix("exports/");

        // Client (also used by the export activities, which need a client to the same backend).
        DurableTaskGrpcClientBuilder clientBuilder = new DurableTaskGrpcClientBuilder();
        DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(clientBuilder, schedulerConnectionString);
        DurableTaskClient client = clientBuilder.build();

        // Worker: register a sample orchestration plus the export entity/orchestrators/activities.
        DurableTaskGrpcWorkerBuilder workerBuilder = new DurableTaskGrpcWorkerBuilder();
        DurableTaskSchedulerWorkerExtensions.useDurableTaskScheduler(workerBuilder, schedulerConnectionString);
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return ORCHESTRATION_NAME;
            }

            @Override
            public TaskOrchestration create() {
                return ctx -> ctx.complete(ctx.getInput(String.class));
            }
        });
        ExportHistoryWorkerExtensions.useExportHistory(workerBuilder, storage, client);

        try (DurableTaskClient autoCloseClient = client;
             DurableTaskGrpcWorker worker = workerBuilder.build()) {
            worker.start();
            logger.info("Worker started.");

            Instant windowStart = Instant.now().minusSeconds(60);

            List<String> instanceIds = new ArrayList<>();
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                instanceIds.add(autoCloseClient.scheduleNewOrchestrationInstance(ORCHESTRATION_NAME, "payload-" + i));
            }
            for (String id : instanceIds) {
                OrchestrationMetadata md = autoCloseClient.waitForInstanceCompletion(id, Duration.ofSeconds(30), false);
                logger.info("Instance " + id + " -> " + md.getRuntimeStatus());
            }

            Instant windowEnd = Instant.now();

            ExportHistoryClient export = ExportHistoryClientExtensions.useExportHistory(autoCloseClient, storage);
            ExportHistoryJobClient jobClient = export.createJob(new ExportJobCreationOptions("sample-export")
                    .setMode(ExportMode.BATCH)
                    .setCompletedTimeFrom(windowStart)
                    .setCompletedTimeTo(windowEnd));

            logger.info("Created export job: " + jobClient.getJobId());

            ExportJobDescription description = jobClient.describe();
            Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
            while (Instant.now().isBefore(deadline)
                    && description.getStatus() != ExportJobStatus.COMPLETED
                    && description.getStatus() != ExportJobStatus.FAILED) {
                Thread.sleep(2000);
                description = jobClient.describe();
            }

            logger.info("=========== RESULTS ===========");
            logger.info("Job status:        " + description.getStatus());
            logger.info("Scanned instances: " + description.getScannedInstances());
            logger.info("Exported instances:" + description.getExportedInstances());
            if (description.getLastError() != null) {
                logger.warning("Last error:        " + description.getLastError());
            }

            if (description.getStatus() != ExportJobStatus.COMPLETED) {
                logger.severe("FAIL: export job did not complete. Status: " + description.getStatus());
                System.exit(1);
            }
            logger.info("Export complete. Archived history written to container '" + container + "' under 'exports/'.");
        }
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isEmpty()) ? defaultValue : value;
    }
}
