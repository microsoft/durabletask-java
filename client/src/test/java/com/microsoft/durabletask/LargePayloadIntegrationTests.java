// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the large payload externalization feature.
 * <p>
 * These tests require a DTS emulator to be running on localhost:4001.
 * They verify end-to-end round-trip of large payloads through externalization.
 */
@Tag("integration")
public class LargePayloadIntegrationTests extends IntegrationTestBase {

    static final Duration defaultTimeout = Duration.ofSeconds(100);

    /**
     * In-memory implementation of {@link PayloadStore} for integration testing.
     * Stores payloads in a thread-safe map with token format {@code test:<key>}.
     */
    static class InMemoryPayloadStore implements PayloadStore {
        private static final String TOKEN_PREFIX = "test:";
        private final ConcurrentHashMap<String, String> payloads = new ConcurrentHashMap<>();
        private final AtomicInteger uploadCount = new AtomicInteger();
        private final AtomicInteger downloadCount = new AtomicInteger();

        @Override
        public String upload(String payload) {
            String key = TOKEN_PREFIX + uploadCount.incrementAndGet();
            payloads.put(key, payload);
            return key;
        }

        @Override
        public String download(String token) {
            downloadCount.incrementAndGet();
            String payload = payloads.get(token);
            if (payload == null) {
                throw new IllegalArgumentException("Unknown token: " + token);
            }
            return payload;
        }

        @Override
        public boolean isKnownPayloadToken(String value) {
            return value != null && value.startsWith(TOKEN_PREFIX);
        }

        int getUploadCount() {
            return uploadCount.get();
        }

        int getDownloadCount() {
            return downloadCount.get();
        }
    }

    private static String generateLargeString(int sizeBytes) {
        StringBuilder sb = new StringBuilder(sizeBytes + 2);
        sb.append('"');
        for (int i = 0; i < sizeBytes - 2; i++) {
            sb.append('A');
        }
        sb.append('"');
        return sb.toString();
    }

    @Test
    void largeOrchestrationInput_isExternalized() throws TimeoutException {
        final String orchestratorName = "LargeInputOrch";
        // Create a payload larger than the default 900KB threshold
        final String largeInput = generateLargeString(1_000_000);

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            String input = ctx.getInput(String.class);
            ctx.complete(input.length());
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, largeInput);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(largeInput.length(), instance.readOutputAs(Integer.class));
            assertTrue(store.getUploadCount() > 0, "Should have externalized at least one payload");
        }
    }

    @Test
    void largeActivityOutput_isExternalized() throws TimeoutException {
        final String orchestratorName = "LargeActivityOutputOrch";
        final String activityName = "GenerateLargeOutput";
        final int payloadSize = 1_000_000;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            String result = ctx.callActivity(activityName, null, String.class).await();
            ctx.complete(result.length());
        });
        workerBuilder.addActivity(activityName, ctx -> {
            StringBuilder sb = new StringBuilder(payloadSize);
            for (int i = 0; i < payloadSize; i++) {
                sb.append('B');
            }
            return sb.toString();
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(payloadSize, instance.readOutputAs(Integer.class));
            assertTrue(store.getUploadCount() > 0, "Should have externalized at least one payload");
        }
    }

    @Test
    void smallPayload_isNotExternalized() throws TimeoutException {
        final String orchestratorName = "SmallPayloadOrch";
        final String smallInput = "Hello, World!";

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            ctx.complete(ctx.getInput(String.class));
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, smallInput);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(smallInput, instance.readOutputAs(String.class));
            assertEquals(0, store.getUploadCount(), "Small payloads should not be externalized");
        }
    }

    @Test
    void largeOrchestrationOutput_isExternalized() throws TimeoutException {
        final String orchestratorName = "LargeOutputOrch";
        final int payloadSize = 1_000_000;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            StringBuilder sb = new StringBuilder(payloadSize);
            for (int i = 0; i < payloadSize; i++) {
                sb.append('C');
            }
            ctx.complete(sb.toString());
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            String output = instance.readOutputAs(String.class);
            assertEquals(payloadSize, output.length());
            assertTrue(store.getUploadCount() > 0, "Large output should be externalized");
        }
    }

    @Test
    void largeTerminateReason_isExternalized() throws TimeoutException {
        final String orchestratorName = "LargeTerminateOrch";
        // Create a reason larger than the threshold
        final String largeReason = generateLargeString(1_000_000);

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            // Wait indefinitely — this will be terminated
            ctx.createTimer(Duration.ofHours(1)).await();
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            client.waitForInstanceStart(instanceId, defaultTimeout);

            client.terminate(instanceId, largeReason);

            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.TERMINATED, instance.getRuntimeStatus());
            assertTrue(store.getUploadCount() > 0, "Large terminate reason should be externalized");
        }
    }

    @Test
    void largeExternalEvent_isExternalized() throws TimeoutException {
        final String orchestratorName = "LargeEventOrch";
        final String eventName = "MyEvent";
        final String largeEventData = generateLargeString(1_000_000);

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            String eventData = ctx.waitForExternalEvent(eventName, String.class).await();
            ctx.complete(eventData.length());
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            client.waitForInstanceStart(instanceId, defaultTimeout);

            client.raiseEvent(instanceId, eventName, largeEventData);

            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(largeEventData.length(), instance.readOutputAs(Integer.class));
            assertTrue(store.getUploadCount() > 0, "Large event data should be externalized");
        }
    }

    @Test
    void largeSubOrchestrationInput_isExternalized() throws TimeoutException {
        final String parentOrchName = "ParentOrch";
        final String childOrchName = "ChildOrch";
        final int payloadSize = 1_000_000;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(parentOrchName, ctx -> {
            StringBuilder sb = new StringBuilder(payloadSize);
            for (int i = 0; i < payloadSize; i++) {
                sb.append('D');
            }
            String result = ctx.callSubOrchestrator(childOrchName, sb.toString(), String.class).await();
            ctx.complete(result);
        });
        workerBuilder.addOrchestrator(childOrchName, ctx -> {
            String input = ctx.getInput(String.class);
            ctx.complete("length=" + input.length());
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(parentOrchName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals("length=" + payloadSize, instance.readOutputAs(String.class));
            assertTrue(store.getUploadCount() > 0, "Large sub-orchestration payload should be externalized");
        }
    }

    @Test
    void largeActivityInput_isExternalized() throws TimeoutException {
        final String orchestratorName = "LargeActivityInputOrch";
        final String activityName = "ProcessLargeInput";
        final int payloadSize = 1_000_000;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            StringBuilder sb = new StringBuilder(payloadSize);
            for (int i = 0; i < payloadSize; i++) {
                sb.append('E');
            }
            Integer result = ctx.callActivity(activityName, sb.toString(), Integer.class).await();
            ctx.complete(result);
        });
        workerBuilder.addActivity(activityName, ctx -> {
            String input = ctx.getInput(String.class);
            return input.length();
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(payloadSize, instance.readOutputAs(Integer.class));
            assertTrue(store.getUploadCount() > 0, "Large activity input should be externalized");
        }
    }

    @Test
    void continueAsNew_withLargeInput_isExternalized() throws TimeoutException {
        final String orchestratorName = "ContinueAsNewOrch";
        final int maxIterations = 3;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            int iteration = ctx.getInput(Integer.class);
            if (iteration >= maxIterations) {
                ctx.complete("done-" + iteration);
                return;
            }
            ctx.continueAsNew(iteration + 1);
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 1);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals("done-" + maxIterations, instance.readOutputAs(String.class));
        }
    }

    @Test
    void queryInstances_resolvesLargePayloads() throws TimeoutException {
        final String orchestratorName = "QueryLargePayloadOrch";
        final int payloadSize = 1_000_000;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            StringBuilder sb = new StringBuilder(payloadSize);
            for (int i = 0; i < payloadSize; i++) {
                sb.append('F');
            }
            ctx.complete(sb.toString());
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata completedInstance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);
            assertNotNull(completedInstance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, completedInstance.getRuntimeStatus());

            // Query the instance to verify payload resolution
            OrchestrationMetadata queried = client.getInstanceMetadata(instanceId, true);
            assertNotNull(queried);
            String output = queried.readOutputAs(String.class);
            assertEquals(payloadSize, output.length());
        }
    }

    @Test
    void suspendAndResume_withLargeReason_works() throws TimeoutException {
        final String orchestratorName = "SuspendResumeOrch";
        final String largeReason = generateLargeString(1_000_000);

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            ctx.waitForExternalEvent("continue").await();
            ctx.complete("resumed");
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            client.waitForInstanceStart(instanceId, defaultTimeout);

            // Suspend with large reason
            client.suspendInstance(instanceId, largeReason);
            Thread.sleep(2000); // allow time for suspend to take effect

            OrchestrationMetadata suspended = client.getInstanceMetadata(instanceId, false);
            assertNotNull(suspended);
            assertEquals(OrchestrationRuntimeStatus.SUSPENDED, suspended.getRuntimeStatus());

            // Resume with large reason
            client.resumeInstance(instanceId, largeReason);
            Thread.sleep(2000);

            // Send event to complete
            client.raiseEvent(instanceId, "continue", null);

            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertTrue(store.getUploadCount() > 0, "Large suspend/resume reasons should be externalized");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Test interrupted");
        }
    }

    // ---- Autochunk tests (matches .NET AutochunkTests) ----

    @Test
    void autochunk_multipleChunks_completesSuccessfully() throws TimeoutException {
        final String orchestratorName = "AutochunkMultipleOrch";
        final String activityName = "GeneratePayload";
        // Use 36 activities, each returning ~30KB. At 1MB chunk size this forces multiple chunks.
        final int activityCount = 36;
        final int payloadSizePerActivity = 30_000;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        // Set a small chunk size to force chunking
        workerBuilder.innerBuilder.setCompleteOrchestratorResponseChunkSizeBytes(
            DurableTaskGrpcWorkerBuilder.MIN_CHUNK_SIZE_BYTES); // 1 MiB
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            List<Task<String>> tasks = IntStream.range(0, activityCount)
                .mapToObj(i -> ctx.callActivity(activityName, i, String.class))
                .collect(Collectors.toList());
            ctx.allOf(tasks).await();
            ctx.complete(activityCount);
        });
        workerBuilder.addActivity(activityName, ctx -> {
            StringBuilder sb = new StringBuilder(payloadSizePerActivity);
            for (int i = 0; i < payloadSizePerActivity; i++) {
                sb.append('X');
            }
            return sb.toString();
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(activityCount, instance.readOutputAs(Integer.class));
        }
    }

    @Test
    void autochunk_mixedActions_completesSuccessfully() throws TimeoutException {
        final String orchestratorName = "AutochunkMixedOrch";
        final String activityName = "MixedActivity";
        final String childOrchName = "MixedChildOrch";
        // 30 activities + many timers + sub-orchestrations
        final int activityCount = 30;
        final int timerCount = 20;
        final int subOrchCount = 10;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.innerBuilder.setCompleteOrchestratorResponseChunkSizeBytes(
            DurableTaskGrpcWorkerBuilder.MIN_CHUNK_SIZE_BYTES);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            int total = 0;

            // Schedule activities
            List<Task<Integer>> activityTasks = IntStream.range(0, activityCount)
                .mapToObj(i -> ctx.callActivity(activityName, i, Integer.class))
                .collect(Collectors.toList());

            // Schedule timers
            for (int i = 0; i < timerCount; i++) {
                ctx.createTimer(Duration.ofMillis(1)).await();
            }

            // Schedule sub-orchestrations
            List<Task<Integer>> subOrchTasks = IntStream.range(0, subOrchCount)
                .mapToObj(i -> ctx.callSubOrchestrator(childOrchName, i, Integer.class))
                .collect(Collectors.toList());

            // Await all activities
            ctx.allOf(activityTasks).await();
            for (Task<Integer> t : activityTasks) {
                total += t.await();
            }

            // Await all sub-orchestrations
            ctx.allOf(subOrchTasks).await();
            for (Task<Integer> t : subOrchTasks) {
                total += t.await();
            }

            ctx.complete(total);
        });
        workerBuilder.addOrchestrator(childOrchName, ctx -> {
            int input = ctx.getInput(Integer.class);
            ctx.complete(input);
        });
        workerBuilder.addActivity(activityName, ctx -> {
            return ctx.getInput(Integer.class);
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            int expectedTotal = 0;
            for (int i = 0; i < activityCount; i++) expectedTotal += i;
            for (int i = 0; i < subOrchCount; i++) expectedTotal += i;
            assertEquals(expectedTotal, instance.readOutputAs(Integer.class));
        }
    }

    @Test
    void autochunk_singleActionExceedsChunkSize_failsWithClearError() throws TimeoutException {
        final String orchestratorName = "AutochunkOversizedOrch";
        // Create an orchestrator that completes with a payload larger than 1MB chunk size.
        // Externalization is NOT configured so the large payload stays inline in the
        // CompleteOrchestration action, which exceeds the chunk size.
        final int payloadSize = 1_200_000;

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.setCompleteOrchestratorResponseChunkSizeBytes(
            DurableTaskGrpcWorkerBuilder.MIN_CHUNK_SIZE_BYTES);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            StringBuilder sb = new StringBuilder(payloadSize);
            for (int i = 0; i < payloadSize; i++) {
                sb.append('Z');
            }
            ctx.complete(sb.toString());
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            // The orchestration should fail because a single action exceeds the chunk size
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());
        }
    }

    // ---- Combined scenario tests (matches .NET combined tests) ----

    @Test
    void largeInputOutputAndCustomStatus_allExternalized() throws TimeoutException {
        final String orchestratorName = "LargeAllFieldsOrch";
        final int payloadSize = 1_000_000;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            String input = ctx.getInput(String.class);

            // Set a large custom status
            StringBuilder customStatus = new StringBuilder(payloadSize);
            for (int i = 0; i < payloadSize; i++) {
                customStatus.append('S');
            }
            ctx.setCustomStatus(customStatus.toString());

            // Return large output
            ctx.complete(input + input);
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String largeInput = generateLargeString(payloadSize);
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, largeInput);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            String output = instance.readOutputAs(String.class);
            assertEquals(largeInput + largeInput, output);
            assertTrue(store.getUploadCount() >= 1, "Should have externalized input, output, and/or custom status");
            assertTrue(store.getDownloadCount() >= 1, "Should have resolved at least one payload");
        }
    }

    @Test
    void continueAsNew_withLargeCustomStatusAndFinalOutput() throws TimeoutException {
        final String orchestratorName = "ContinueAsNewAllOrch";
        final int payloadSize = 1_000_000;
        final int iterations = 3;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            int iteration = ctx.getInput(Integer.class);

            // Set large custom status on every iteration
            StringBuilder status = new StringBuilder(payloadSize);
            for (int i = 0; i < payloadSize; i++) {
                status.append((char) ('A' + (iteration % 26)));
            }
            ctx.setCustomStatus(status.toString());

            if (iteration >= iterations) {
                // Large final output
                StringBuilder finalOutput = new StringBuilder(payloadSize);
                for (int i = 0; i < payloadSize; i++) {
                    finalOutput.append('F');
                }
                ctx.complete(finalOutput.toString());
                return;
            }
            ctx.continueAsNew(iteration + 1);
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 1);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            String output = instance.readOutputAs(String.class);
            assertEquals(payloadSize, output.length());
            assertTrue(store.getUploadCount() > 0, "Should have externalized custom status and/or final output");
        }
    }

    @Test
    void largeSubOrchestrationAndActivityOutput_combined() throws TimeoutException {
        final String parentOrchName = "CombinedParentOrch";
        final String childOrchName = "CombinedChildOrch";
        final String activityName = "CombinedActivity";
        final int subOrchPayloadSize = 1_000_000;
        final int activityPayloadSize = 1_000_000;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(parentOrchName, ctx -> {
            // Large sub-orchestration input
            StringBuilder subInput = new StringBuilder(subOrchPayloadSize);
            for (int i = 0; i < subOrchPayloadSize; i++) {
                subInput.append('G');
            }
            String subResult = ctx.callSubOrchestrator(childOrchName, subInput.toString(), String.class).await();

            // Activity that returns large output
            String actResult = ctx.callActivity(activityName, null, String.class).await();

            ctx.complete(subResult.length() + actResult.length());
        });
        workerBuilder.addOrchestrator(childOrchName, ctx -> {
            String input = ctx.getInput(String.class);
            ctx.complete("child-" + input.length());
        });
        workerBuilder.addActivity(activityName, ctx -> {
            StringBuilder sb = new StringBuilder(activityPayloadSize);
            for (int i = 0; i < activityPayloadSize; i++) {
                sb.append('H');
            }
            return sb.toString();
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(parentOrchName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            int expectedLength = ("child-" + subOrchPayloadSize).length() + activityPayloadSize;
            assertEquals(expectedLength, instance.readOutputAs(Integer.class));
            assertTrue(store.getUploadCount() >= 1, "Should have externalized large payloads");
            assertTrue(store.getDownloadCount() >= 1, "Should have resolved large payloads");
        }
    }

    // ---- Max payload rejection integration test ----

    @Test
    void exceedingMaxPayload_isRejected() throws TimeoutException {
        final String orchestratorName = "MaxPayloadRejectionOrch";
        // Use a very small max so we can trigger rejection without massive strings
        final int threshold = 100;
        final int maxPayload = 200;

        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder()
            .setThresholdBytes(threshold)
            .setMaxExternalizedPayloadBytes(maxPayload)
            .build();

        TestDurableTaskWorkerBuilder workerBuilder = this.createWorkerBuilder();
        workerBuilder.innerBuilder.useExternalizedPayloads(store, options);
        workerBuilder.addOrchestrator(orchestratorName, ctx -> {
            // Generate output that exceeds max externalized payload size
            StringBuilder sb = new StringBuilder(500);
            for (int i = 0; i < 500; i++) {
                sb.append('R');
            }
            ctx.complete(sb.toString());
        });
        DurableTaskGrpcWorker worker = workerBuilder.buildAndStart();

        DurableTaskGrpcClientBuilder clientBuilder = this.createClientBuilder();
        clientBuilder.useExternalizedPayloads(store, options);
        DurableTaskClient client = clientBuilder.build();

        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

            assertNotNull(instance);
            // The orchestration should fail because the payload exceeds max
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());
        }
    }
}
