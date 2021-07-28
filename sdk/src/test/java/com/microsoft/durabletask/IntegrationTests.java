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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class IntegrationTests {
    static final Duration defaultTimeout = Duration.ofSeconds(100);

    // All tests that create a server should save it to this variable for proper shutdown
    private TaskHubServer server;

    @AfterEach
    private void shutdown() throws InterruptedException {
        if (this.server != null) {
            this.server.stop();
        }
    }

    @Test
    void emptyOrchestration() throws IOException {
        final String orchestratorName = "EmptyOrchestration";
        final String input = "Hello " + Instant.now();
        this.createServerBuilder()
            .addOrchestrator(orchestratorName, ctx -> ctx.complete(ctx.getInput(String.class)))
            .buildAndStart();

        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, input);
        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(
            instanceId,
            defaultTimeout,
            true);
        
        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(input, instance.getInputAs(String.class));
        assertEquals(input, instance.getOutputAs(String.class));
    }

    @Test
    void singleTimer() throws IOException {
        final String orchestratorName = "SingleTimer";
        final Duration delay = Duration.ofSeconds(3);
        this.createServerBuilder()
            .addOrchestrator(orchestratorName, ctx -> ctx.createTimer(delay).get())
            .buildAndStart();

        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
        Duration timeout = delay.plus(defaultTimeout);
        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(instanceId, timeout, false);
        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

        // Verify that the delay actually happened
        long expectedCompletionSecond = instance.getCreatedTime().plus(delay).getEpochSecond();
        long actualCompletionSecond = instance.getLastUpdatedTime().getEpochSecond();
        assertTrue(expectedCompletionSecond <= actualCompletionSecond);
    }

    @Test
    public void isReplaying() throws IOException, InterruptedException {
        final String orchestratorName = "SingleTimer";
        this.createServerBuilder()
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

        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(
            instanceId,
            defaultTimeout,
            true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

        // Verify that the orchestrator reported the correct isReplaying values.
        // Note that only the values of the *final* replay are returned.
        List<?> results = instance.getOutputAs(List.class);
        assertEquals(3, results.size());
        assertTrue((Boolean)results.get(0));
        assertTrue((Boolean)results.get(1));
        assertFalse((Boolean)results.get(2));
    }

    @Test
    void singleActivity() throws IOException, InterruptedException {
        final String orchestratorName = "SingleActivity";
        final String activityName = "Echo";
        final String input = Instant.now().toString();
        this.createServerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                String activityInput = ctx.getInput(String.class);
                String output = ctx.callActivity(activityName, activityInput, String.class).get();
                ctx.complete(output);
            })
            .addActivity(activityName, ctx -> {
                return String.format("Hello, %s!", ctx.getInput(String.class));
            })
            .buildAndStart();

        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, input);
        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(
            instanceId,
            defaultTimeout,
            true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        String output = instance.getOutputAs(String.class);
        String expected = String.format("Hello, %s!", input);
        assertEquals(expected, output);
    }

    @Test
    void currentDateTimeUtc() throws IOException {
        final String orchestratorName = "CurrentDateTimeUtc";
        final String echoActivityName = "Echo";

        this.createServerBuilder()
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

        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertTrue(instance.getOutputAs(boolean.class));
    }

    @Test
    void activityChain() throws IOException {
        final String orchestratorName = "ActivityChain";
        final String plusOneActivityName = "PlusOne";

        this.createServerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                int value = ctx.getInput(int.class);
                for (int i = 0; i < 10; i++) {
                    value = ctx.callActivity(plusOneActivityName, i, int.class).get();
                }

                ctx.complete(value);
            })
            .addActivity(plusOneActivityName, ctx -> ctx.getInput(int.class) + 1)
            .buildAndStart();

        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(10, instance.getOutputAs(int.class));
    }

    @Test
    void orchestratorException() throws IOException {
        final String orchestratorName = "OrchestratorWithException";
        final String errorMessage = "Kah-BOOOOOM!!!";

        this.createServerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                throw new RuntimeException(errorMessage);
            })
            .buildAndStart();

        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

        ErrorDetails details = instance.getOutputAs(ErrorDetails.class);
        assertNotNull(details);
        assertTrue(details.getFullText().contains(errorMessage));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void activityException(boolean handleException) throws IOException {
        final String orchestratorName = "OrchestratorWithActivityException";
        final String activityName = "Throw";
        final String errorMessage = "Kah-BOOOOOM!!!";

        this.createServerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                try {
                    ctx.callActivity(activityName).get();
                } catch (TaskFailedException ex) {
                    if (handleException) {
                        ctx.complete(ex.getMessage());
                    } else {
                        throw ex;
                    }
                }
            })
            .addActivity(activityName, ctx -> {
                throw new RuntimeException(errorMessage);
            })
            .buildAndStart();

        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
        assertNotNull(instance);

        if (handleException) {
            String expected = String.format(
                    "Activity '%s' with task ID 0 failed: java.lang.RuntimeException: %s",
                    activityName,
                    errorMessage);
            String actual = instance.getOutputAs(String.class);
            assertTrue(actual.startsWith(expected));
        } else {
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            ErrorDetails details = instance.getOutputAs(ErrorDetails.class);
            assertNotNull(details);
            assertTrue(details.getFullText().contains(errorMessage));
        }
    }

    @Test
    void activityFanOut() throws IOException {
        final String orchestratorName = "ActivityFanOut";
        final String activityName = "ToString";
        final int activityCount = 10;

        this.createServerBuilder()
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


        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

        List<?> output = instance.getOutputAs(List.class);
        assertNotNull(output);
        assertEquals(activityCount, output.size());
        assertEquals(String.class, output.get(0).getClass());

        // Expected: ["9", "8", "7", "6", "5", "4", "3", "2", "1", "0"]
        for (int i = 0; i < activityCount; i++) {
            String expected = String.valueOf(activityCount - i - 1);
            assertEquals(expected, output.get(i).toString());
        }
    }

    @Test
    void externalEvents() throws IOException {
        final String orchestratorName = "ExternalEvents";
        final String eventName = "MyEvent";
        final int eventCount = 10;

        this.createServerBuilder()
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

        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);

        for (int i = 0; i < eventCount; i++) {
            client.raiseEvent(instanceId, eventName, i);
        }

        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

        int output = instance.getOutputAs(int.class);
        assertEquals(eventCount, output);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void externalEventsWithTimeouts(boolean raiseEvent) throws IOException {
        final String orchestratorName = "ExternalEventsWithTimeouts";
        final String eventName = "MyEvent";

        this.createServerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                try {
                    ctx.waitForExternalEvent(eventName, Duration.ofSeconds(3)).get();
                    ctx.complete("received");
                } catch (TaskCanceledException e) {
                    ctx.complete(e.getMessage());
                }
            })
            .buildAndStart();

        TaskHubClient client = TaskHubClient.newBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);

        client.waitForInstanceStart(instanceId, defaultTimeout);
        if (raiseEvent) {
            client.raiseEvent(instanceId, eventName);
        }

        TaskOrchestrationInstance instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

        String output = instance.getOutputAs(String.class);
        if (raiseEvent) {
            assertEquals("received", output);
        } else {
            assertEquals("Timeout of PT3S expired while waiting for an event named '" + eventName + "' (ID = 0).", output);
        }
    }

    private TestTaskHubServerBuilder createServerBuilder() {
        return new TestTaskHubServerBuilder();
    }

    public class TestTaskHubServerBuilder {
        final TaskHubServer.Builder innerBuilder;

        private TestTaskHubServerBuilder() {
            this.innerBuilder = TaskHubServer.newBuilder();
        }

        public TaskHubServer buildAndStart() throws IOException {
            TaskHubServer server = this.innerBuilder.build();
            IntegrationTests.this.server = server;
            server.start();
            return server;
        }

        public TestTaskHubServerBuilder addOrchestrator(
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

        public <R> TestTaskHubServerBuilder addActivity(
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
