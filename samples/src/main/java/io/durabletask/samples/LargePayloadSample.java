// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azureblobpayloads.*;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerClientExtensions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerExtensions;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Demonstrates large payload externalization with the Durable Task SDK.
 *
 * <p>This sample schedules an orchestration with a payload larger than 1 MiB.
 * The worker echoes it through an activity the SDK transparently externalizes
 * the payload to Azure Blob Storage and resolves it back on the other side.
 *
 * <h3>Prerequisites</h3>
 * <ol>
 *   <li>DTS emulator: {@code docker run -d -p 8080:8080 mcr.microsoft.com/dts/dts-emulator:latest}</li>
 *   <li>Azurite: {@code docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0}</li>
 * </ol>
 *
 * <h3>Running</h3>
 * <pre>
 *   ./gradlew :samples:runLargePayloadSample
 * </pre>
 *
 * <h3>Environment variables</h3>
 * <ul>
 *   <li>{@code DURABLE_TASK_SCHEDULER_CONNECTION_STRING} — DTS connection string
 *       (default: {@code Endpoint=http://localhost:8080;TaskHub=default;Authentication=None})</li>
 *   <li>{@code PAYLOAD_STORAGE_CONNECTION_STRING} — Azure Storage connection string
 *       (default: {@code UseDevelopmentStorage=true})</li>
 *   <li>{@code PAYLOAD_SIZE_BYTES} — payload size in bytes (default: 1572864 = 1.5 MiB)</li>
 *   <li>{@code EXTERNALIZE_THRESHOLD_BYTES} — externalization threshold (default: 900000)</li>
 * </ul>
 */
final class LargePayloadSample {

    private static final Logger logger = Logger.getLogger(LargePayloadSample.class.getName());
    private static final String ORCHESTRATION_NAME = "LargePayloadRoundTrip";
    private static final String ACTIVITY_NAME = "EchoPayload";

    public static void main(String[] args) throws InterruptedException, TimeoutException {
        // --- Configuration ---
        String schedulerConnectionString = envOrDefault(
            "DURABLE_TASK_SCHEDULER_CONNECTION_STRING",
            "Endpoint=http://localhost:8080;TaskHub=default;Authentication=None");

        String storageConnectionString = envOrDefault(
            "PAYLOAD_STORAGE_CONNECTION_STRING",
            "UseDevelopmentStorage=true");

        int payloadSizeBytes = intEnvOrDefault("PAYLOAD_SIZE_BYTES", 1572864); // 1.5 MiB

        int externalizeThresholdBytes = intEnvOrDefault("EXTERNALIZE_THRESHOLD_BYTES", 900000);

        logger.info(String.format(
            "Config: payloadSize=%d bytes, threshold=%d bytes",
            payloadSizeBytes, externalizeThresholdBytes));

        // --- Payload storage options ---
        LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
            .setConnectionString(storageConnectionString)
            .setThresholdBytes(externalizeThresholdBytes);

        // Create a shared PayloadStore for both client and worker
        PayloadStore payloadStore = new BlobPayloadStore(payloadOptions);

        // --- Client ---
        DurableTaskGrpcClientBuilder clientBuilder = new DurableTaskGrpcClientBuilder();
        DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(clientBuilder, schedulerConnectionString);
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, payloadStore, payloadOptions);

        // --- Worker ---
        DurableTaskGrpcWorkerBuilder workerBuilder = new DurableTaskGrpcWorkerBuilder();
        DurableTaskSchedulerWorkerExtensions.useDurableTaskScheduler(workerBuilder, schedulerConnectionString);
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, payloadStore, payloadOptions);

        // Register orchestration: passes payload through an activity and returns the echo
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return ORCHESTRATION_NAME; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    String echoed = ctx.callActivity(ACTIVITY_NAME, input, String.class).await();
                    ctx.complete(echoed);
                };
            }
        });

        // Register activity: echoes the payload back
        workerBuilder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return ACTIVITY_NAME; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    String payload = ctx.getInput(String.class);
                    if (payload != null && payload.startsWith("blob:v1:")) {
                        throw new IllegalStateException(
                            "Activity received a blob token instead of the resolved payload!");
                    }
                    return payload;
                };
            }
        });

        // --- Run ---
        try (DurableTaskClient client = clientBuilder.build();
             DurableTaskGrpcWorker worker = workerBuilder.build()) {

            worker.start();
            logger.info("Worker started.");

            // Generate a deterministic, low-compressibility payload
            String payload = createPayload(payloadSizeBytes);
            int payloadUtf8Bytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            boolean exceedsOneMiB = payloadUtf8Bytes > 1024 * 1024;

            logger.info(String.format("Scheduling orchestration with %d byte payload (exceeds 1 MiB: %s)",
                payloadUtf8Bytes, exceedsOneMiB));

            String instanceId = client.scheduleNewOrchestrationInstance(ORCHESTRATION_NAME, payload);
            logger.info("Instance ID: " + instanceId);

            // Wait for completion
            OrchestrationMetadata result = client.waitForInstanceCompletion(
                instanceId, Duration.ofSeconds(120), true);

            String outputPayload = result.readOutputAs(String.class);
            boolean roundTripMatched = payload.equals(outputPayload);

            logger.info("=========== RESULTS ===========");
            logger.info("Runtime status:    " + result.getRuntimeStatus());
            logger.info("Payload bytes:     " + payloadUtf8Bytes);
            logger.info("Exceeds 1 MiB:     " + exceedsOneMiB);
            logger.info("Threshold bytes:   " + externalizeThresholdBytes);
            logger.info("Round-trip matched: " + roundTripMatched);

            if (!roundTripMatched) {
                logger.severe("FAIL: Round-trip payload mismatch!");
                System.exit(1);
            }
            if (result.getRuntimeStatus() != OrchestrationRuntimeStatus.COMPLETED) {
                logger.severe("FAIL: Orchestration did not complete. Status: " + result.getRuntimeStatus());
                if (result.getFailureDetails() != null) {
                    logger.severe("Failure: " + result.getFailureDetails().getErrorMessage());
                }
                System.exit(1);
            }

            logger.info("SUCCESS: Large payload round-trip verified.");
        }
    }

    /**
     * Creates a deterministic, low-compressibility payload of the given size.
     * Uses the same LCG algorithm as the .NET sample for cross-SDK comparability.
     */
    private static String createPayload(int sizeBytes) {
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        char[] chars = new char[sizeBytes];
        int state = 0x00C0FFEE;
        for (int i = 0; i < sizeBytes; i++) {
            state = (state * 1664525) + 1013904223;
            chars[i] = alphabet.charAt((state >>> 26) & 0x3F);
        }
        return new String(chars);
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    private static int intEnvOrDefault(String key, int defaultValue) {
        String rawValue = System.getenv(key);
        if (rawValue == null || rawValue.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(rawValue);
        } catch (NumberFormatException e) {
            logger.warning(String.format(
                "Invalid integer value for %s: '%s'. Using default value: %d.",
                key, rawValue, defaultValue));
            return defaultValue;
        }
    }

}
