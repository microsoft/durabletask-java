// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * These integration tests are designed to exercise the core, high-level features of
 * the Durable Task programming model.
 * <p/>
 * These tests currently require a sidecar process to be
 * running on the local machine (the sidecar is what accepts the client operations and
 * sends invocation instructions to the DurableTaskWorker).
 */
@Tag("integration")
public class IntegrationTests extends IntegrationTestBase {
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

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
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
            .addOrchestrator(orchestratorName, ctx -> ctx.createTimer(delay).await())
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
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
                ctx.createTimer(Duration.ofSeconds(0)).await();
                list.add(ctx.getIsReplaying());
                ctx.createTimer(Duration.ofSeconds(0)).await();
                list.add(ctx.getIsReplaying());
                ctx.complete(list);
            })
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
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
                String output = ctx.callActivity(activityName, activityInput, String.class).await();
                ctx.complete(output);
            })
            .addActivity(activityName, ctx -> {
                return String.format("Hello, %s!", ctx.getInput(String.class));
            })
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
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
                Instant originalInstant1 = ctx.callActivity(echoActivityName, currentInstant1, Instant.class).await();
                if (!currentInstant1.equals(originalInstant1)) {
                    ctx.complete(false);
                    return;
                }

                Instant currentInstant2 = ctx.getCurrentInstant();
                Instant originalInstant2 = ctx.callActivity(echoActivityName, currentInstant2, Instant.class).await();
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

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
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
                    value = ctx.callActivity(plusOneActivityName, i, int.class).await();
                }

                ctx.complete(value);
            })
            .addActivity(plusOneActivityName, ctx -> ctx.getInput(int.class) + 1)
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(10, instance.readOutputAs(int.class));
        }
    }

    @Test
    void subOrchestration(){
        final String orchestratorName = "SubOrchestration";
        DurableTaskGrpcWorker worker = this.createWorkerBuilder().addOrchestrator(orchestratorName, ctx -> {
            int result = 5;
            int input = ctx.getInput(int.class);
            if (input < 3){
                result += ctx.callSubOrchestrator(orchestratorName, input + 1, int.class).await();
            }
            ctx.complete(result);
        }).buildAndStart();
        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try(worker; client){
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 1);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(15, instance.readOutputAs(int.class));
        }
    }

    @Test
    void continueAsNew(){
        final String orchestratorName = "continueAsNew";
        final Duration delay = Duration.ofSeconds(0);
        DurableTaskGrpcWorker worker = this.createWorkerBuilder().addOrchestrator(orchestratorName, ctx -> {
            int input = ctx.getInput(int.class);
            if (input < 10){
                ctx.createTimer(delay).await();
                ctx.continueAsNew(input + 1);
            } else {
                ctx.complete(input);
            }
        }).buildAndStart();
        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try(worker; client){
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 1);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(10, instance.readOutputAs(int.class));
        }
    }

    @Test
    void continueAsNewWithExternalEvents() {
        final String orchestratorName = "continueAsNewWithExternalEvents";
        final String eventName = "MyEvent";
        final int expectedEventCount = 10;
        final Duration delay = Duration.ofSeconds(0);
        DurableTaskGrpcWorker worker = this.createWorkerBuilder().addOrchestrator(orchestratorName, ctx -> {
            int receivedEventCount = ctx.getInput(int.class);

            if (receivedEventCount < expectedEventCount) {
                ctx.waitForExternalEvent(eventName, int.class).await();
                ctx.continueAsNew(receivedEventCount + 1, true);
            } else {
                ctx.complete(receivedEventCount);
            }
        }).buildAndStart();
        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);

            for (int i = 0; i < expectedEventCount; i++) {
                client.raiseEvent(instanceId, eventName, i);
            }

            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals(expectedEventCount, instance.readOutputAs(int.class));
        }
    }

    @Test
    void termination() {
        final String orchestratorName = "Termination";
        final Duration delay = Duration.ofSeconds(3);

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> ctx.createTimer(delay).await())
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            String expectOutput = "I'll be back.";
            client.terminate(instanceId, expectOutput);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(instanceId, instance.getInstanceId());
            assertEquals(OrchestrationRuntimeStatus.TERMINATED, instance.getRuntimeStatus());
            assertEquals(expectOutput, instance.readOutputAs(String.class));
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
                List<String> results = ctx.allOf(parallelTasks).await();
                Collections.sort(results);
                Collections.reverse(results);
                ctx.complete(results);
            })
            .addActivity(activityName, ctx -> ctx.getInput(Object.class).toString())
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
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
                    int payload = ctx.waitForExternalEvent(eventName, int.class).await();
                    if (payload != i) {
                        ctx.complete(-1);
                        return;
                    }
                }

                ctx.complete(i);
            })
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
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
                    ctx.waitForExternalEvent(eventName, Duration.ofSeconds(3)).await();
                    ctx.complete("received");
                } catch (TaskCanceledException e) {
                    ctx.complete(e.getMessage());
                }
            })
            .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
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

    @Test
    void setCustomStatus() {
        final String orchestratorName = "SetCustomStatus";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    ctx.setCustomStatus("Started!");
                    Object customStatus = ctx.waitForExternalEvent("StatusEvent", Object.class).await();
                    ctx.setCustomStatus(customStatus);
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);

            OrchestrationMetadata metadata = client.waitForInstanceStart(instanceId, defaultTimeout, true);
            assertNotNull(metadata);
            assertEquals("Started!", metadata.readCustomStatusAs(String.class));

            Map<String, Integer> payload = new HashMap<String ,Integer>(){{
                put("Hello",45);
            }};
            client.raiseEvent(metadata.getInstanceId(), "StatusEvent", payload);

            metadata = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(metadata);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, metadata.getRuntimeStatus());
            assertTrue(metadata.isCustomStatusFetched());
            assertEquals(payload, metadata.readCustomStatusAs(HashMap.class));
        }
    }

    @Test
    void clearCustomStatus() {
        final String orchestratorName = "ClearCustomStatus";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    ctx.setCustomStatus("Started!");
                    ctx.waitForExternalEvent("StatusEvent").await();
                    ctx.clearCustomStatus();
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);

            OrchestrationMetadata metadata = client.waitForInstanceStart(instanceId, defaultTimeout, true);
            assertNotNull(metadata);
            assertEquals("Started!", metadata.readCustomStatusAs(String.class));

            client.raiseEvent(metadata.getInstanceId(), "StatusEvent");

            metadata = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(metadata);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, metadata.getRuntimeStatus());
            assertFalse(metadata.isCustomStatusFetched());
        }
    }

    @Test
    void multiInstanceQuery() {
        final String plusOne = "plusOne";
        final String waitForEvent = "waitForEvent";
        final DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(plusOne, ctx -> {
                    int value = ctx.getInput(int.class);
                    for (int i = 0; i < 10; i++) {
                        value = ctx.callActivity(plusOne, value, int.class).await();
                    }
                    ctx.complete(value);
                })
                .addActivity(plusOne, ctx -> ctx.getInput(int.class) + 1)
                .addOrchestrator(waitForEvent, ctx ->{
                    String name = ctx.getInput(String.class);
                    String output = ctx.waitForExternalEvent(name, String.class).await();
                    ctx.complete(output);
                }).buildAndStart();

        try(worker; client){
            client.createTaskHub(true);
            Instant startTime = Instant.now();
            String prefix = startTime.toString();

            IntStream.range(0, 5).mapToObj(i -> {
                String instanceId = String.format("%s.sequence.%d", prefix, i);
                client.scheduleNewOrchestrationInstance(plusOne, 0, instanceId);
                return instanceId;
            }).collect(Collectors.toUnmodifiableList()).forEach(id -> {
                client.waitForInstanceCompletion(id, defaultTimeout, true);
            });

            Instant sequencesFinishedTime = Instant.now();

            IntStream.range(0, 5).mapToObj(i -> {
                String instanceId = String.format("%s.waiter.%d", prefix, i);
                client.scheduleNewOrchestrationInstance(waitForEvent, String.valueOf(i), instanceId);
                return instanceId;
            }).collect(Collectors.toUnmodifiableList()).forEach(id -> {
                client.waitForInstanceStart(id, defaultTimeout);
            });

            // Create one query object and reuse it for multiple queries
            OrchestrationStatusQuery query = new OrchestrationStatusQuery();
            OrchestrationStatusQueryResult result = null;

            // Return all instances
            result = client.queryInstances(query);
            assertEquals(10, result.getOrchestrationState().size());

            // Test CreatedTimeTo filter
            query.setCreatedTimeTo(startTime);
            result = client.queryInstances(query);
            assertTrue(result.getOrchestrationState().isEmpty());

            query.setCreatedTimeTo(sequencesFinishedTime);
            result = client.queryInstances(query);
            assertEquals(5, result.getOrchestrationState().size());

            query.setCreatedTimeTo(Instant.now());
            result = client.queryInstances(query);
            assertEquals(10, result.getOrchestrationState().size());

            // Test CreatedTimeFrom filter
            query.setCreatedTimeFrom(Instant.now());
            result = client.queryInstances(query);
            assertTrue(result.getOrchestrationState().isEmpty());

            query.setCreatedTimeFrom(sequencesFinishedTime);
            result = client.queryInstances(query);
            assertEquals(5, result.getOrchestrationState().size());

            query.setCreatedTimeFrom(startTime);
            result = client.queryInstances(query);
            assertEquals(10, result.getOrchestrationState().size());

            // Test RuntimeStatus filter
            HashSet<OrchestrationRuntimeStatus> statusFilters = Stream.of(
                    OrchestrationRuntimeStatus.PENDING,
                    OrchestrationRuntimeStatus.FAILED,
                    OrchestrationRuntimeStatus.TERMINATED
            ).collect(Collectors.toCollection(HashSet::new));

            query.setRuntimeStatusList(new ArrayList<>(statusFilters));
            result = client.queryInstances(query);
            assertTrue(result.getOrchestrationState().isEmpty());

            statusFilters.add(OrchestrationRuntimeStatus.RUNNING);
            query.setRuntimeStatusList(new ArrayList<>(statusFilters));
            result = client.queryInstances(query);
            assertEquals(5, result.getOrchestrationState().size());

            statusFilters.add(OrchestrationRuntimeStatus.COMPLETED);
            query.setRuntimeStatusList(new ArrayList<>(statusFilters));
            result = client.queryInstances(query);
            assertEquals(10, result.getOrchestrationState().size());

            statusFilters.remove(OrchestrationRuntimeStatus.RUNNING);
            query.setRuntimeStatusList(new ArrayList<>(statusFilters));
            result = client.queryInstances(query);
            assertEquals(5, result.getOrchestrationState().size());

            statusFilters.clear();
            query.setRuntimeStatusList(new ArrayList<>(statusFilters));
            result = client.queryInstances(query);
            assertEquals(10, result.getOrchestrationState().size());

            // Test InstanceIdPrefix
            query.setInstanceIdPrefix("Foo");
            result = client.queryInstances(query);
            assertTrue(result.getOrchestrationState().isEmpty());

            query.setInstanceIdPrefix(prefix);
            result = client.queryInstances(query);
            assertEquals(10, result.getOrchestrationState().size());

            // Test PageSize and ContinuationToken
            HashSet<String> instanceIds = new HashSet<>();
            query.setMaxInstanceCount(0);
            while(query.getMaxInstanceCount() < 10){
                query.setMaxInstanceCount(query.getMaxInstanceCount()+1);
                result = client.queryInstances(query);
                int total = result.getOrchestrationState().size();
                assertEquals(query.getMaxInstanceCount(), total);
                result.getOrchestrationState().forEach(state -> assertTrue(instanceIds.add(state.getInstanceId())));
                while (total < 10){
                    query.setContinuationToken(result.getContinuationToken());
                    result = client.queryInstances(query);
                    int count = result.getOrchestrationState().size();
                    assertNotEquals(0, count);
                    assertTrue(count <= query.getMaxInstanceCount());
                    total += count;
                    assertTrue(total <= 10);
                    result.getOrchestrationState().forEach(state -> assertTrue(instanceIds.add(state.getInstanceId())));
                }
                query.setContinuationToken(null);
                instanceIds.clear();
            }

            // Test ShowInput
            query.setFetchInputsAndOutputs(true);
            query.setCreatedTimeFrom(sequencesFinishedTime);
            result = client.queryInstances(query);
            result.getOrchestrationState().forEach(state -> assertNotNull(state.readInputAs(String.class)));

            query.setFetchInputsAndOutputs(false);
            query.setCreatedTimeFrom(sequencesFinishedTime);
            result = client.queryInstances(query);
            result.getOrchestrationState().forEach(state -> assertThrows(IllegalStateException.class, () -> state.readInputAs(String.class)));
        }
    }

    @Test
    void purgeInstanceId() {
        final String orchestratorName = "PurgeInstance";
        final String plusOneActivityName = "PlusOne";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    int value = ctx.getInput(int.class);
                    value = ctx.callActivity(plusOneActivityName, value, int.class).await();
                    ctx.complete(value);
                })
                .addActivity(plusOneActivityName, ctx -> ctx.getInput(int.class) + 1)
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            client.createTaskHub(true);
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata metadata = client.waitForInstanceCompletion(instanceId,  defaultTimeout, true);
            assertNotNull(metadata);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, metadata.getRuntimeStatus());
            assertEquals(1, metadata.readOutputAs(int.class));

            PurgeResult result = client.purgeInstances(instanceId);
            assertEquals(1, result.getDeletedInstanceCount());

            metadata = client.getInstanceMetadata(instanceId, true);
            assertFalse(metadata.isInstancePresent());
        }
    }

    @Test
    void purgeInstanceFilter() {
        final String orchestratorName = "PurgeInstance";
        final String plusOne = "PlusOne";
        final String plusTwo = "PlusTwo";
        final String terminate = "Termination";

        final Duration delay = Duration.ofSeconds(1);

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    int value = ctx.getInput(int.class);
                    value = ctx.callActivity(plusOne, value, int.class).await();
                    ctx.complete(value);
                })
                .addActivity(plusOne, ctx -> ctx.getInput(int.class) + 1)
                .addOrchestrator(plusOne, ctx -> {
                    int value = ctx.getInput(int.class);
                    value = ctx.callActivity(plusOne, value, int.class).await();
                    ctx.complete(value);
                })
                .addOrchestrator(plusTwo, ctx -> {
                    int value = ctx.getInput(int.class);
                    value = ctx.callActivity(plusTwo, value, int.class).await();
                    ctx.complete(value);
                })
                .addActivity(plusTwo, ctx -> ctx.getInput(int.class) + 2)
                .addOrchestrator(terminate, ctx -> ctx.createTimer(delay).await())
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            client.createTaskHub(true);
            Instant startTime = Instant.now();

            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata metadata = client.waitForInstanceCompletion(instanceId,  defaultTimeout, true);
            assertNotNull(metadata);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, metadata.getRuntimeStatus());
            assertEquals(1, metadata.readOutputAs(int.class));

            // Test CreatedTimeFrom
            PurgeInstanceCriteria criteria = new PurgeInstanceCriteria();
            criteria.setCreatedTimeFrom(startTime);

            PurgeResult result = client.purgeInstances(criteria);
            assertEquals(1, result.getDeletedInstanceCount());
            metadata = client.getInstanceMetadata(instanceId, true);
            assertFalse(metadata.isInstancePresent());

            // Test CreatedTimeTo
            criteria.setCreatedTimeTo(Instant.now());

            result = client.purgeInstances(criteria);
            assertEquals(0, result.getDeletedInstanceCount());
            metadata = client.getInstanceMetadata(instanceId, true);
            assertFalse(metadata.isInstancePresent());

            // Test CreatedTimeFrom, CreatedTimeTo, and RuntimeStatus
            String instanceId1 = client.scheduleNewOrchestrationInstance(plusOne, 0);
            metadata = client.waitForInstanceCompletion(instanceId1,  defaultTimeout, true);
            assertNotNull(metadata);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, metadata.getRuntimeStatus());
            assertEquals(1, metadata.readOutputAs(int.class));

            String instanceId2 = client.scheduleNewOrchestrationInstance(plusTwo, 10);
            metadata = client.waitForInstanceCompletion(instanceId2,  defaultTimeout, true);
            assertNotNull(metadata);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, metadata.getRuntimeStatus());
            assertEquals(12, metadata.readOutputAs(int.class));

            String instanceId3 = client.scheduleNewOrchestrationInstance(terminate);
            client.terminate(instanceId3, terminate);
            metadata = client.waitForInstanceCompletion(instanceId3, defaultTimeout, true);
            assertNotNull(metadata);
            assertEquals(OrchestrationRuntimeStatus.TERMINATED, metadata.getRuntimeStatus());
            assertEquals(terminate, metadata.readOutputAs(String.class));

            HashSet<OrchestrationRuntimeStatus> runtimeStatusFilters = Stream.of(
                    OrchestrationRuntimeStatus.TERMINATED,
                    OrchestrationRuntimeStatus.COMPLETED
            ).collect(Collectors.toCollection(HashSet::new));

            criteria.setCreatedTimeTo(Instant.now());
            criteria.setRuntimeStatusList(new ArrayList<>(runtimeStatusFilters));
            result = client.purgeInstances(criteria);

            assertEquals(3, result.getDeletedInstanceCount());
            metadata = client.getInstanceMetadata(instanceId1, true);
            assertFalse(metadata.isInstancePresent());
            metadata = client.getInstanceMetadata(instanceId2, true);
            assertFalse(metadata.isInstancePresent());
            metadata = client.getInstanceMetadata(instanceId3, true);
            assertFalse(metadata.isInstancePresent());
        }
    }
}
