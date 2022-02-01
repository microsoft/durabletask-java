// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * These integration tests are designed to exercise the core, high-level features of
 * the Durable Task programming model.
 * <p/>
 * These tests currently require a sidecar process to be
 * running on the local machine (the sidecar is what accepts the client operations and
 * sends invocation instructions to the DurableTaskWorker).
 */
@Tag("integration")
public class IntegrationTests {
    static final Duration defaultTimeout = Duration.ofSeconds(100);

    // All tests that create a server should save it to this variable for proper shutdown
    private DurableTaskGrpcWorker server;

    @AfterEach
    private void shutdown() throws InterruptedException {
        if (this.server != null) {
            this.server.stop();
        }
    }

    @Test
    void emptyOrchestration() {
        final String orchestratorName = "EmptyOrchestration";
        final String input = "Hello " + Instant.now();
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> ctx.complete(ctx.getInput(String.class)))
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, input);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId,
                defaultTimeout,
                true);
            
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(input, instance.readInputAs(String.class));
            assertEquals(input, instance.readOutputAs(String.class));
        }
    }

    @Test
    void singleTimer() throws IOException {
        final String orchestratorName = "SingleTimer";
        final Duration delay = Duration.ofSeconds(3);
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> ctx.createTimer(delay).get())
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            Duration timeout = delay.plus(defaultTimeout);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, timeout, false);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            // Verify that the delay actually happened
            long expectedCompletionSecond = instance.getCreatedAt().plus(delay).getEpochSecond();
            long actualCompletionSecond = instance.getLastUpdatedAt().getEpochSecond();
            assertTrue(expectedCompletionSecond <= actualCompletionSecond);
        }
    }

    @Test
    void isReplaying() throws IOException, InterruptedException {
        final String orchestratorName = "SingleTimer";
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                ArrayList<Boolean> list = new ArrayList<Boolean>();
                list.add(ctx.getIsReplaying());
                ctx.createTimer(Duration.ofSeconds(0)).get();
                list.add(ctx.getIsReplaying());
                ctx.createTimer(Duration.ofSeconds(0)).get();
                list.add(ctx.getIsReplaying());
                ctx.complete(list);
            })
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId,
                defaultTimeout,
                true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            // Verify that the orchestrator reported the correct isReplaying values.
            // Note that only the values of the *final* replay are returned.
            List<?> results = instance.readOutputAs(List.class);
            assertEquals(3, results.size());
            assertTrue((Boolean)results.get(0));
            assertTrue((Boolean)results.get(1));
            assertFalse((Boolean)results.get(2));
        }
    }

    @Test
    void singleActivity() throws IOException, InterruptedException {
        final String orchestratorName = "SingleActivity";
        final String activityName = "Echo";
        final String input = Instant.now().toString();
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                String activityInput = ctx.getInput(String.class);
                String output = ctx.callActivity(activityName, activityInput, String.class).get();
                ctx.complete(output);
            })
            .addActivity(activityName, ctx -> {
                return String.format("Hello, %s!", ctx.getInput(String.class));
            })
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, input);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId,
                defaultTimeout,
                true);

            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            String output = instance.readOutputAs(String.class);
            String expected = String.format("Hello, %s!", input);
            assertEquals(expected, output);
        }
    }

    @Test
    void currentDateTimeUtc() throws IOException {
        final String orchestratorName = "CurrentDateTimeUtc";
        final String echoActivityName = "Echo";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                Instant currentInstant1 = ctx.getCurrentInstant();
                Instant originalInstant1 = ctx.callActivity(echoActivityName, currentInstant1, Instant.class).get();
                if (!currentInstant1.equals(originalInstant1)) {
                    ctx.complete(false);
                    return;
                }

                Instant currentInstant2 = ctx.getCurrentInstant();
                Instant originalInstant2 = ctx.callActivity(echoActivityName, currentInstant2, Instant.class).get();
                if (!currentInstant2.equals(originalInstant2)) {
                    ctx.complete(false);
                    return;
                }

                ctx.complete(!currentInstant1.equals(currentInstant2));
            })
            .addActivity(echoActivityName, ctx -> {
                // Return the input back to the caller, regardless of its type
                return ctx.getInput(Object.class);
            })
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertTrue(instance.readOutputAs(boolean.class));
        }
    }

    @Test
    void activityChain() throws IOException {
        final String orchestratorName = "ActivityChain";
        final String plusOneActivityName = "PlusOne";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                int value = ctx.getInput(int.class);
                for (int i = 0; i < 10; i++) {
                    value = ctx.callActivity(plusOneActivityName, i, int.class).get();
                }

                ctx.complete(value);
            })
            .addActivity(plusOneActivityName, ctx -> ctx.getInput(int.class) + 1)
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(10, instance.readOutputAs(int.class));
        }
    }

    @Test
    void orchestratorException() throws IOException {
        final String orchestratorName = "OrchestratorWithException";
        final String errorMessage = "Kah-BOOOOOM!!!";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                throw new RuntimeException(errorMessage);
            })
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            ErrorDetails details = instance.readOutputAs(ErrorDetails.class);
            assertNotNull(details);
            assertTrue(details.getErrorDetails().contains(errorMessage));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void activityException(boolean handleException) throws IOException {
        final String orchestratorName = "OrchestratorWithActivityException";
        final String activityName = "Throw";
        final String errorMessage = "Kah-BOOOOOM!!!";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                try {
                    ctx.callActivity(activityName).get();
                } catch (TaskFailedException ex) {
                    if (handleException) {
                        ctx.complete("handled");
                    } else {
                        throw ex;
                    }
                }
            })
            .addActivity(activityName, ctx -> {
                throw new RuntimeException(errorMessage);
            })
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, "");
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);

            if (handleException) {
                String result = instance.readOutputAs(String.class);
                assertNotNull(result);
                assertEquals("handled", result);
            } else {
                assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

                ErrorDetails details = instance.readOutputAs(ErrorDetails.class);
                assertNotNull(details);

                String expectedMessage = String.format(
                    "Activity task '%s' with ID 0 failed with an unhandled exception.",
                    activityName,
                    errorMessage);
                assertEquals(expectedMessage, details.getErrorMessage());
                assertEquals("com.microsoft.durabletask.TaskFailedException", details.getErrorName());
                assertNotNull(details.getErrorDetails());
                // CONSIDER: Additional validation of getErrorDetails?
            }
        }
    }

    @Test
    void activityFanOut() throws IOException {
        final String orchestratorName = "ActivityFanOut";
        final String activityName = "ToString";
        final int activityCount = 10;

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                // Schedule each task to run in parallel
                List<Task<String>> parallelTasks = IntStream.range(0, activityCount)
                        .mapToObj(i -> ctx.callActivity(activityName, i, String.class))
                        .collect(Collectors.toList());

                // Wait for all tasks to complete, then sort and reverse the results
                List<String> results = ctx.allOf(parallelTasks).get();
                Collections.sort(results);
                Collections.reverse(results);
                ctx.complete(results);
            })
            .addActivity(activityName, ctx -> ctx.getInput(Object.class).toString())
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            List<?> output = instance.readOutputAs(List.class);
            assertNotNull(output);
            assertEquals(activityCount, output.size());
            assertEquals(String.class, output.get(0).getClass());

            // Expected: ["9", "8", "7", "6", "5", "4", "3", "2", "1", "0"]
            for (int i = 0; i < activityCount; i++) {
                String expected = String.valueOf(activityCount - i - 1);
                assertEquals(expected, output.get(i).toString());
            }
        }
    }

    @Test
    void externalEvents() throws IOException {
        final String orchestratorName = "ExternalEvents";
        final String eventName = "MyEvent";
        final int eventCount = 10;

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                int i;
                for (i = 0; i < eventCount; i++) {
                    // block until the event is received
                    int payload = ctx.waitForExternalEvent(eventName, int.class).get();
                    if (payload != i) {
                        ctx.complete(-1);
                        return;
                    }
                }

                ctx.complete(i);
            })
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);

            for (int i = 0; i < eventCount; i++) {
                client.raiseEvent(instanceId, eventName, i);
            }

            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            int output = instance.readOutputAs(int.class);
            assertEquals(eventCount, output);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void externalEventsWithTimeouts(boolean raiseEvent) throws IOException {
        final String orchestratorName = "ExternalEventsWithTimeouts";
        final String eventName = "MyEvent";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                try {
                    ctx.waitForExternalEvent(eventName, Duration.ofSeconds(3)).get();
                    ctx.complete("received");
                } catch (TaskCanceledException e) {
                    ctx.complete(e.getMessage());
                }
            })
            .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);

            client.waitForInstanceStart(instanceId, defaultTimeout);
            if (raiseEvent) {
                client.raiseEvent(instanceId, eventName);
            }

            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            String output = instance.readOutputAs(String.class);
            if (raiseEvent) {
                assertEquals("received", output);
            } else {
                assertEquals("Timeout of PT3S expired while waiting for an event named '" + eventName + "' (ID = 0).", output);
            }
        }
    }

    private TestDurableTaskWorkerBuilder createWorkerBuilder() {
        return new TestDurableTaskWorkerBuilder();
    }

    public class TestDurableTaskWorkerBuilder {
        final DurableTaskGrpcWorker.Builder innerBuilder;

        private TestDurableTaskWorkerBuilder() {
            this.innerBuilder = DurableTaskGrpcWorker.newBuilder();
        }

        public DurableTaskGrpcWorker buildAndStart() {
            DurableTaskGrpcWorker server = this.innerBuilder.build();
            IntegrationTests.this.server = server;
            server.start();
            return server;
        }

        public TestDurableTaskWorkerBuilder addOrchestrator(
                String name,
                TaskOrchestration implementation) {
            this.innerBuilder.addOrchestration(new TaskOrchestrationFactory() {
                @Override
                public String getName() { return name; }

                @Override
                public TaskOrchestration create() { return implementation; }
            });
            return this;
        }

        public <R> TestDurableTaskWorkerBuilder addActivity(
            String name,
            TaskActivity implementation)
        {
            this.innerBuilder.addActivity(new TaskActivityFactory() {
                @Override
                public String getName() { return name; }

                @Override
                public TaskActivity create() { return implementation; }
            });
            return this;
        }
    }
}
