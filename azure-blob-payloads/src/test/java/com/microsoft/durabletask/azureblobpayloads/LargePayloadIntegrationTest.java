// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerClientOptions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerOptions;

import io.grpc.Channel;
import io.grpc.ManagedChannel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the large payload externalization feature.
 * <p>
 * These tests require:
 * <ul>
 *   <li>DTS emulator running on localhost:4001:
 *       {@code docker run --name durabletask-emulator -p 4001:8080 -d mcr.microsoft.com/dts/dts-emulator:latest}</li>
 *   <li>Azurite running on localhost:10000:
 *       {@code docker run --name azurite -p 10000:10000 -p 10001:10001 -p 10002:10002 -d mcr.microsoft.com/azure-storage/azurite}</li>
 * </ul>
 */
@Tag("integration")
public class LargePayloadIntegrationTest {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final String AZURITE_CONNECTION_STRING =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    private static final String EMULATOR_ENDPOINT =
        System.getenv("DTS_ENDPOINT") != null
            ? System.getenv("DTS_ENDPOINT")
            : "http://localhost:8080";

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
            try { client.close(); } catch (Exception e) { /* ignore */ }
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
    void roundTrip_largeInput_activityEchoesBack() throws TimeoutException {
        // Arrange: create a 1.5 MB payload that exceeds the default 900KB threshold
        String largePayload = generatePayload(1_500_000);

        LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
            .setConnectionString(AZURITE_CONNECTION_STRING)
            .setThresholdBytes(900_000);

        PayloadStore store = new BlobPayloadStore(payloadOptions);

        // Set up worker with LP enabled
        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "EchoOrchestration"; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    String result = ctx.callActivity("EchoActivity", input, String.class).await();
                    ctx.complete(result);
                };
            }
        });
        workerBuilder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "EchoActivity"; }
            @Override
            public TaskActivity create() { return ctx -> ctx.getInput(String.class); }
        });

        worker = workerBuilder.build();
        worker.start();

        // Set up client with LP enabled
        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        // Act
        String instanceId = client.scheduleNewOrchestrationInstance("EchoOrchestration", largePayload);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, DEFAULT_TIMEOUT, true);

        // Assert
        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        String output = instance.readOutputAs(String.class);
        assertEquals(largePayload, output, "Round-trip payload should match");
    }

    @Test
    void roundTrip_smallInput_passedInline() throws TimeoutException {
        // Small payloads should pass through without being externalized
        String smallPayload = "Hello, small world!";

        LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
            .setConnectionString(AZURITE_CONNECTION_STRING)
            .setThresholdBytes(900_000);

        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "SmallEchoOrchestration"; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    ctx.complete(input);
                };
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("SmallEchoOrchestration", smallPayload);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, DEFAULT_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(smallPayload, instance.readOutputAs(String.class));
    }

    @Test
    void chunking_fanOutOrchestration_manyActivities() throws TimeoutException {
        // Fan-out orchestration that schedules many activities producing medium-sized outputs
        // This tests that chunked completion works end-to-end.
        int activityCount = 10;
        String mediumPayload = generatePayload(100_000); // 100 KB each

        LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
            .setConnectionString(AZURITE_CONNECTION_STRING)
            .setThresholdBytes(900_000);

        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "FanOutOrchestration"; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    // Schedule many parallel activities
                    java.util.List<Task<String>> tasks = new java.util.ArrayList<>();
                    for (int i = 0; i < activityCount; i++) {
                        tasks.add(ctx.callActivity("GeneratePayload", i, String.class));
                    }
                    // Wait for all
                    java.util.List<String> results = new java.util.ArrayList<>();
                    for (Task<String> task : tasks) {
                        results.add(task.await());
                    }
                    ctx.complete(results.size());
                };
            }
        });
        workerBuilder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "GeneratePayload"; }
            @Override
            public TaskActivity create() {
                return ctx -> mediumPayload;
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("FanOutOrchestration");
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, Duration.ofSeconds(120), true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(activityCount, instance.readOutputAs(Integer.class));
    }

    // ==================== Sub-orchestration ====================

    @Test
    void subOrchestration_largeInputAndOutput() throws TimeoutException {
        String largePayload = generatePayload(1_500_000);

        LargePayloadStorageOptions payloadOptions = createDefaultPayloadOptions();
        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "ParentOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    String result = ctx.callSubOrchestrator(
                        "ChildOrchestration", input, String.class).await();
                    ctx.complete(result);
                };
            }
        });
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "ChildOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    ctx.complete(input);
                };
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("ParentOrchestration", largePayload);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, Duration.ofSeconds(60), true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(largePayload, instance.readOutputAs(String.class));
    }

    // ==================== Raise event ====================

    @Test
    void raiseEvent_largeEventPayload() throws TimeoutException {
        String largeEventData = generatePayload(1_500_000);

        LargePayloadStorageOptions payloadOptions = createDefaultPayloadOptions();
        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "WaitForEventOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    String eventData = ctx.waitForExternalEvent(
                        "LargeEvent", Duration.ofSeconds(30), String.class).await();
                    ctx.complete(eventData);
                };
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("WaitForEventOrchestration");

        // Wait briefly for the orchestration to start before raising the event
        try { Thread.sleep(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        client.raiseEvent(instanceId, "LargeEvent", largeEventData);

        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, Duration.ofSeconds(60), true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(largeEventData, instance.readOutputAs(String.class));
    }

    // ==================== Large orchestration output ====================

    @Test
    void largeOrchestrationOutput_directCompletion() throws TimeoutException {
        String largeOutput = generatePayload(1_500_000);

        LargePayloadStorageOptions payloadOptions = createDefaultPayloadOptions();
        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "LargeOutputOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> ctx.complete(largeOutput);
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("LargeOutputOrchestration");
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, DEFAULT_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(largeOutput, instance.readOutputAs(String.class));
    }

    // ==================== Query instances ====================

    @Test
    void queryInstances_resolvesLargePayloadTokens() throws TimeoutException {
        String largePayload = generatePayload(1_500_000);

        LargePayloadStorageOptions payloadOptions = createDefaultPayloadOptions();
        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "QueryTestOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> ctx.complete(ctx.getInput(String.class));
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("QueryTestOrchestration", largePayload);
        client.waitForInstanceCompletion(instanceId, DEFAULT_TIMEOUT, true);

        // Query by instance ID and verify input/output are resolved
        OrchestrationMetadata metadata = client.getInstanceMetadata(instanceId, true);

        assertNotNull(metadata);
        assertEquals(largePayload, metadata.readInputAs(String.class),
            "Query should resolve input payload token");
        assertEquals(largePayload, metadata.readOutputAs(String.class),
            "Query should resolve output payload token");
    }

    // ==================== Continue-as-new ====================

    @Test
    void continueAsNew_largeInput() throws TimeoutException {
        String largePayload = generatePayload(1_500_000);

        LargePayloadStorageOptions payloadOptions = createDefaultPayloadOptions();
        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        // Orchestration that does continue-as-new on first run, completes on second
        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "ContinueAsNewOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    int iteration = ctx.getInput(int[].class) != null
                        ? ctx.getInput(int[].class)[0] : 0;
                    if (iteration == 0) {
                        // First run: continue-as-new with a large-payload activity result
                        String result = ctx.callActivity("GetLargeData", null, String.class).await();
                        // Pass iteration counter + result hash to prove data survived
                        ctx.continueAsNew(new int[]{ 1, result.length() });
                    } else {
                        // Second run: complete with the iteration info
                        ctx.complete(iteration);
                    }
                };
            }
        });
        workerBuilder.addActivity(new TaskActivityFactory() {
            @Override public String getName() { return "GetLargeData"; }
            @Override public TaskActivity create() { return ctx -> largePayload; }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance(
            "ContinueAsNewOrchestration", new int[]{ 0 });
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, Duration.ofSeconds(60), true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        // Second iteration should have completed with iteration=1
        assertEquals(1, instance.readOutputAs(Integer.class));
    }

    // ==================== Max payload exceeded ====================

    @Test
    void maxPayloadExceeded_activityFailsWithNonRetriableError() throws TimeoutException {
        LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
            .setConnectionString(AZURITE_CONNECTION_STRING)
            .setThresholdBytes(100)         // very low threshold to trigger externalization
            .setMaxPayloadBytes(500_000);   // 500 KB max — activity will produce > 500 KB

        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "MaxPayloadOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    try {
                        String result = ctx.callActivity(
                            "OversizedActivity", null, String.class).await();
                        ctx.complete(result);
                    } catch (TaskFailedException e) {
                        ctx.complete("FAILED:" + e.getErrorDetails().getErrorType());
                    }
                };
            }
        });
        workerBuilder.addActivity(new TaskActivityFactory() {
            @Override public String getName() { return "OversizedActivity"; }
            @Override public TaskActivity create() {
                return ctx -> generatePayload(1_000_000); // 1 MB > 500 KB max
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("MaxPayloadOrchestration");
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, DEFAULT_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        String output = instance.readOutputAs(String.class);
        assertTrue(output.startsWith("FAILED:"),
            "Orchestration should catch the non-retriable failure: " + output);
    }

    // ==================== Compression disabled ====================

    @Test
    void compressionDisabled_roundTrip() throws TimeoutException {
        String largePayload = generatePayload(1_500_000);

        LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
            .setConnectionString(AZURITE_CONNECTION_STRING)
            .setThresholdBytes(900_000)
            .setCompressionEnabled(false);

        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "NoCompressionOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    String input = ctx.getInput(String.class);
                    String result = ctx.callActivity("EchoNoCompress", input, String.class).await();
                    ctx.complete(result);
                };
            }
        });
        workerBuilder.addActivity(new TaskActivityFactory() {
            @Override public String getName() { return "EchoNoCompress"; }
            @Override public TaskActivity create() { return ctx -> ctx.getInput(String.class); }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("NoCompressionOrchestration", largePayload);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, DEFAULT_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(largePayload, instance.readOutputAs(String.class));
    }

    // ==================== Custom status ====================

    @Test
    void largeCustomStatus_externalizedAndResolved() throws TimeoutException {
        String largeCustomStatus = generatePayload(1_500_000);

        LargePayloadStorageOptions payloadOptions = createDefaultPayloadOptions();
        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "CustomStatusOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    ctx.setCustomStatus(largeCustomStatus);
                    ctx.complete("done");
                };
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("CustomStatusOrchestration");
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, DEFAULT_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(largeCustomStatus, instance.readCustomStatusAs(String.class));
    }

    // ==================== Orchestration output exceeds max ====================

    @Test
    void orchestrationOutputExceedsMax_failsGracefully() throws TimeoutException {
        LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
            .setConnectionString(AZURITE_CONNECTION_STRING)
            .setThresholdBytes(100)
            .setMaxPayloadBytes(500_000);

        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "OversizedOutputOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    // Orchestration itself tries to return a payload exceeding max
                    ctx.complete(generatePayload(1_000_000));
                };
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("OversizedOutputOrchestration");
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, DEFAULT_TIMEOUT, true);

        assertNotNull(instance);
        // Orchestrator response externalization failure → non-retriable Failed status
        assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());
    }

    // ==================== Terminate with large payload ====================

    @Test
    void terminate_largeOutput() throws TimeoutException {
        String largeTerminateOutput = generatePayload(1_500_000);

        LargePayloadStorageOptions payloadOptions = createDefaultPayloadOptions();
        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "WaitForeverOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    // Wait for an event that never comes — will be terminated
                    ctx.waitForExternalEvent("NeverArrives", Duration.ofHours(1)).await();
                };
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("WaitForeverOrchestration");

        // Wait for orchestration to start running
        try { Thread.sleep(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Terminate with large output
        client.terminate(instanceId, largeTerminateOutput);

        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, DEFAULT_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.TERMINATED, instance.getRuntimeStatus());
        assertEquals(largeTerminateOutput, instance.readOutputAs(String.class));
    }

    // ==================== Suspend and resume with large reason ====================

    @Test
    void suspendAndResume_largeReason() throws TimeoutException {
        String largeSuspendReason = generatePayload(1_500_000);
        String largeResumeReason = generatePayload(1_200_000);

        LargePayloadStorageOptions payloadOptions = createDefaultPayloadOptions();
        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "SuspendResumeOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    // Wait for an external event so the orchestration stays running
                    // long enough to be suspended
                    String input = ctx.waitForExternalEvent(
                        "ContinueSignal", Duration.ofSeconds(30), String.class).await();
                    ctx.complete(input);
                };
            }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("SuspendResumeOrchestration");

        // Wait for orchestration start
        try { Thread.sleep(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Suspend with large reason — tests SuspendRequest.reason externalization
        client.suspendInstance(instanceId, largeSuspendReason);

        try { Thread.sleep(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        OrchestrationMetadata suspended = client.getInstanceMetadata(instanceId, false);
        assertNotNull(suspended);
        assertEquals(OrchestrationRuntimeStatus.SUSPENDED, suspended.getRuntimeStatus());

        // Resume with large reason — tests ResumeRequest.reason externalization
        client.resumeInstance(instanceId, largeResumeReason);

        try { Thread.sleep(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Send event to let the orchestration complete after resume
        client.raiseEvent(instanceId, "ContinueSignal", "resumed-ok");

        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, DEFAULT_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals("resumed-ok", instance.readOutputAs(String.class));
    }

    // ==================== Large single activity output exceeding chunk size ====================

    @Test
    void largeActivityOutput_exceedsChunkSize_externalizesWithLP() throws TimeoutException {
        // A single activity returning a very large output (5 MB) that would exceed
        // the 3.9 MB chunk limit, but gets externalized by the LP interceptor first
        String veryLargePayload = generatePayload(5_000_000);

        LargePayloadStorageOptions payloadOptions = createDefaultPayloadOptions();
        PayloadStore store = new BlobPayloadStore(payloadOptions);

        DurableTaskGrpcWorkerBuilder workerBuilder = createWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, payloadOptions);

        workerBuilder.addOrchestration(new TaskOrchestrationFactory() {
            @Override public String getName() { return "LargeActivityOutputOrchestration"; }
            @Override public TaskOrchestration create() {
                return ctx -> {
                    String result = ctx.callActivity("VeryLargeActivity", null, String.class).await();
                    ctx.complete(result.length());
                };
            }
        });
        workerBuilder.addActivity(new TaskActivityFactory() {
            @Override public String getName() { return "VeryLargeActivity"; }
            @Override public TaskActivity create() { return ctx -> veryLargePayload; }
        });

        worker = workerBuilder.build();
        worker.start();

        DurableTaskGrpcClientBuilder clientBuilder = createClientBuilder();
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, payloadOptions);
        client = clientBuilder.build();

        String instanceId = client.scheduleNewOrchestrationInstance("LargeActivityOutputOrchestration");
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, Duration.ofSeconds(60), true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(veryLargePayload.length(), instance.readOutputAs(Integer.class));
    }

    // ==================== Helpers ====================

    private LargePayloadStorageOptions createDefaultPayloadOptions() {
        return new LargePayloadStorageOptions()
            .setConnectionString(AZURITE_CONNECTION_STRING)
            .setThresholdBytes(900_000);
    }

    private DurableTaskGrpcWorkerBuilder createWorkerBuilder() {
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress(EMULATOR_ENDPOINT)
            .setTaskHubName("default")
            .setCredential(null)
            .setAllowInsecureCredentials(true);
        Channel grpcChannel = options.createGrpcChannel();
        this.workerChannel = (ManagedChannel) grpcChannel;
        return new DurableTaskGrpcWorkerBuilder()
            .grpcChannel(grpcChannel);
    }

    private DurableTaskGrpcClientBuilder createClientBuilder() {
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions()
            .setEndpointAddress(EMULATOR_ENDPOINT)
            .setTaskHubName("default")
            .setCredential(null)
            .setAllowInsecureCredentials(true);
        Channel grpcChannel = options.createGrpcChannel();
        this.clientChannel = (ManagedChannel) grpcChannel;
        return new DurableTaskGrpcClientBuilder()
            .grpcChannel(grpcChannel);
    }

    private static String generatePayload(int sizeBytes) {
        StringBuilder sb = new StringBuilder(sizeBytes);
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        for (int i = 0; i < sizeBytes; i++) {
            sb.append(chars.charAt(i % chars.length()));
        }
        return sb.toString();
    }
}
