// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.io.IOException;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
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
    void emptyOrchestration() throws TimeoutException {
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
    void singleTimer() throws IOException, TimeoutException {
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
    void longTimer() throws TimeoutException {
        final String orchestratorName = "LongTimer";
        final Duration delay = Duration.ofSeconds(7);
        AtomicInteger counter = new AtomicInteger();
        AtomicReferenceArray<LocalDateTime> timestamps = new AtomicReferenceArray<>(4);
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    timestamps.set(counter.get(), LocalDateTime.now());
                    counter.incrementAndGet();
                    ctx.createTimer(delay).await();
                })
                .setMaximumTimerInterval(Duration.ofSeconds(3))
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

            // Verify that the correct number of timers were created
            // This should yield 4 (first invocation + replay invocations for internal timers 3s + 3s + 1s)
            assertEquals(4, counter.get());

            // Verify that each timer is the expected length
            int[] secondsElapsed = new int[3];
            for (int i = 0; i < timestamps.length() - 1; i++) {
                secondsElapsed[i] = timestamps.get(i + 1).getSecond() - timestamps.get(i).getSecond();
            }
            assertEquals(secondsElapsed[0], 3);
            assertEquals(secondsElapsed[1], 3);
            assertEquals(secondsElapsed[2], 1);
        }
    }

    @Test
    void longTimerNonblocking() throws TimeoutException {
        final String orchestratorName = "ActivityAnyOf";
        final String externalEventActivityName = "externalEvent";
        final String externalEventWinner = "The external event completed first";
        final String timerEventWinner = "The timer event completed first";
        final Duration timerDuration = Duration.ofSeconds(20);
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
            .addOrchestrator(orchestratorName, ctx -> {
                Task<String> externalEvent = ctx.waitForExternalEvent(externalEventActivityName, String.class);
                Task<Void> longTimer = ctx.createTimer(timerDuration);
                Task<?> winnerEvent = ctx.anyOf(externalEvent, longTimer).await();
                if (winnerEvent == externalEvent) {
                    ctx.complete(externalEventWinner);
                } else {
                    ctx.complete(timerEventWinner);
                }
            }).setMaximumTimerInterval(Duration.ofSeconds(3)).buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            client.raiseEvent(instanceId, externalEventActivityName, "Hello world");
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            String output = instance.readOutputAs(String.class);
            assertNotNull(output);
            assertTrue(output.equals(externalEventWinner));

            long createdTime = instance.getCreatedAt().getEpochSecond();
            long completedTime = instance.getLastUpdatedAt().getEpochSecond();
            // Timer did not block execution
            assertTrue(completedTime - createdTime < 5);
        }
    }

    @Test
    void longTimerNonblockingNoExternal() throws TimeoutException {
        final String orchestratorName = "ActivityAnyOf";
        final String externalEventActivityName = "externalEvent";
        final String externalEventWinner = "The external event completed first";
        final String timerEventWinner = "The timer event completed first";
        final Duration timerDuration = Duration.ofSeconds(20);
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    Task<String> externalEvent = ctx.waitForExternalEvent(externalEventActivityName, String.class);
                    Task<Void> longTimer = ctx.createTimer(timerDuration);
                    Task<?> winnerEvent = ctx.anyOf(externalEvent, longTimer).await();
                    if (winnerEvent == externalEvent) {
                        ctx.complete(externalEventWinner);
                    } else {
                        ctx.complete(timerEventWinner);
                    }
                }).setMaximumTimerInterval(Duration.ofSeconds(3)).buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            String output = instance.readOutputAs(String.class);
            assertNotNull(output);
            assertTrue(output.equals(timerEventWinner));

            long expectedCompletionSecond = instance.getCreatedAt().plus(timerDuration).getEpochSecond();
            long actualCompletionSecond = instance.getLastUpdatedAt().getEpochSecond();
            assertTrue(expectedCompletionSecond <= actualCompletionSecond);
        }
    }


    @Test
    void longTimeStampTimer() throws TimeoutException {
        final String orchestratorName = "LongTimeStampTimer";
        final Duration delay = Duration.ofSeconds(7);
        final ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.now().plusSeconds(delay.getSeconds()), ZoneId.systemDefault());

        AtomicInteger counter = new AtomicInteger();
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    counter.incrementAndGet();
                    ctx.createTimer(zonedDateTime).await();
                })
                .setMaximumTimerInterval(Duration.ofSeconds(3))
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            Duration timeout = delay.plus(defaultTimeout);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, timeout, false);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            // Verify that the delay actually happened
            long expectedCompletionSecond = zonedDateTime.toInstant().getEpochSecond();
            long actualCompletionSecond = instance.getLastUpdatedAt().getEpochSecond();
            assertTrue(expectedCompletionSecond <= actualCompletionSecond);

            // Verify that the correct number of timers were created
            // This should yield 4 (first invocation + replay invocations for internal timers 3s + 3s + 1s)
            assertEquals(4, counter.get());
        }
    }

    @Test
    void singleTimeStampTimer() throws IOException, TimeoutException {
        final String orchestratorName = "SingleTimeStampTimer";
        final Duration delay = Duration.ofSeconds(3);
        final ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.now().plusSeconds(delay.getSeconds()), ZoneId.systemDefault());
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> ctx.createTimer(zonedDateTime).await())
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            Duration timeout = delay.plus(defaultTimeout);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, timeout, false);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            // Verify that the delay actually happened
            long expectedCompletionSecond = zonedDateTime.toInstant().getEpochSecond();
            long actualCompletionSecond = instance.getLastUpdatedAt().getEpochSecond();
            assertTrue(expectedCompletionSecond <= actualCompletionSecond);
        }
    }

    @Test
    void isReplaying() throws IOException, InterruptedException, TimeoutException {
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
    void singleActivity() throws IOException, InterruptedException, TimeoutException {
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
    void currentDateTimeUtc() throws IOException, TimeoutException {
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
    void activityChain() throws IOException, TimeoutException {
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
    void subOrchestration() throws TimeoutException {
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
    void continueAsNew() throws TimeoutException {
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
    void continueAsNewWithExternalEvents() throws TimeoutException, InterruptedException{
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
    void termination() throws TimeoutException {
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void restartOrchestrationWithNewInstanceId(boolean restartWithNewInstanceId) throws TimeoutException {
        final String orchestratorName = "restart";
        final Duration delay = Duration.ofSeconds(3);

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> ctx.createTimer(delay).await())
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, "RestartTest");
            client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            String newInstanceId = client.restartInstance(instanceId, restartWithNewInstanceId);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(newInstanceId, defaultTimeout, true);

            if (restartWithNewInstanceId) {
                assertNotEquals(instanceId, newInstanceId);
            } else {
                assertEquals(instanceId, newInstanceId);
            }
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertEquals("\"RestartTest\"", instance.getSerializedInput());
        }
    }

    @Test
    void restartOrchestrationThrowsException() {
        final String orchestratorName = "restart";
        final Duration delay = Duration.ofSeconds(3);
        final String nonExistentId = "123";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> ctx.createTimer(delay).await())
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            client.scheduleNewOrchestrationInstance(orchestratorName, "RestartTest");

            assertThrows(
                IllegalArgumentException.class,
                () -> client.restartInstance(nonExistentId, true)
            );
        }

    }

    @Test
    void suspendResumeOrchestration() throws TimeoutException, InterruptedException {
        final String orchestratorName = "suspend";
        final String eventName = "MyEvent";
        final String eventPayload = "testPayload";
        final Duration suspendTimeout = Duration.ofSeconds(5);

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    String payload = ctx.waitForExternalEvent(eventName, String.class).await();
                    ctx.complete(payload);
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            client.suspendInstance(instanceId);
            OrchestrationMetadata instance = client.waitForInstanceStart(instanceId, defaultTimeout);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.SUSPENDED, instance.getRuntimeStatus());

            client.raiseEvent(instanceId, eventName, eventPayload);

            assertThrows(
                    TimeoutException.class,
                    () -> client.waitForInstanceCompletion(instanceId, suspendTimeout, false),
                    "Expected to throw TimeoutException, but it didn't"
                    );

            String resumeReason = "Resume for testing.";
            client.resumeInstance(instanceId, resumeReason);
            instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(instanceId, instance.getInstanceId());
            assertEquals(eventPayload, instance.readOutputAs(String.class));
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        }
    }

    @Test
    void terminateSuspendOrchestration() throws TimeoutException, InterruptedException {
        final String orchestratorName = "suspendResume";
        final String eventName = "MyEvent";
        final String eventPayload = "testPayload";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    String payload = ctx.waitForExternalEvent(eventName, String.class).await();
                    ctx.complete(payload);
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            String suspendReason = "Suspend for testing.";
            client.suspendInstance(instanceId, suspendReason);
            client.terminate(instanceId, null);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, false);
            assertNotNull(instance);
            assertEquals(instanceId, instance.getInstanceId());
            assertEquals(OrchestrationRuntimeStatus.TERMINATED, instance.getRuntimeStatus());
        }
    }

    @Test
    void activityFanOut() throws IOException, TimeoutException {
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
    void externalEvents() throws IOException, TimeoutException {
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
    void externalEventsWithTimeouts(boolean raiseEvent) throws IOException, TimeoutException {
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
    void setCustomStatus() throws TimeoutException {
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
    void clearCustomStatus() throws TimeoutException {
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
    void multiInstanceQuery() throws TimeoutException{
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
                try {
                    client.waitForInstanceCompletion(id, defaultTimeout, true);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            });

            Instant sequencesFinishedTime = Instant.now();

            IntStream.range(0, 5).mapToObj(i -> {
                String instanceId = String.format("%s.waiter.%d", prefix, i);
                client.scheduleNewOrchestrationInstance(waitForEvent, String.valueOf(i), instanceId);
                return instanceId;
            }).collect(Collectors.toUnmodifiableList()).forEach(id -> {
                try {
                    client.waitForInstanceStart(id, defaultTimeout);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
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
    void purgeInstanceId() throws TimeoutException {
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

            PurgeResult result = client.purgeInstance(instanceId);
            assertEquals(1, result.getDeletedInstanceCount());

            metadata = client.getInstanceMetadata(instanceId, true);
            assertFalse(metadata.isInstanceFound());
        }
    }

    @Test
    void purgeInstanceFilter() throws TimeoutException {
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
            assertFalse(metadata.isInstanceFound());

            // Test CreatedTimeTo
            criteria.setCreatedTimeTo(Instant.now());

            result = client.purgeInstances(criteria);
            assertEquals(0, result.getDeletedInstanceCount());
            metadata = client.getInstanceMetadata(instanceId, true);
            assertFalse(metadata.isInstanceFound());

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
            assertFalse(metadata.isInstanceFound());
            metadata = client.getInstanceMetadata(instanceId2, true);
            assertFalse(metadata.isInstanceFound());
            metadata = client.getInstanceMetadata(instanceId3, true);
            assertFalse(metadata.isInstanceFound());
        }
    }

    @Test
    void purgeInstanceFilterTimeout() throws TimeoutException {
        final String orchestratorName = "PurgeInstance";
        final String plusOne = "PlusOne";
        final String plusTwo = "PlusTwo";

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

            PurgeInstanceCriteria criteria = new PurgeInstanceCriteria();
            criteria.setCreatedTimeFrom(startTime);
            criteria.setTimeout(Duration.ofNanos(1));

            assertThrows(TimeoutException.class, () -> client.purgeInstances(criteria));
        }
    }

    @Test()
    void waitForInstanceStartThrowsException() {
        final String orchestratorName = "orchestratorName";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    try {
                        // The orchestration remains in the "Pending" state until the first await statement
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            assertThrows(TimeoutException.class, () -> client.waitForInstanceStart(instanceId, Duration.ofSeconds(2)));
        }
    }

    @Test()
    void waitForInstanceCompletionThrowsException() {
        final String orchestratorName = "orchestratorName";
        final String plusOneActivityName = "PlusOne";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    int value = ctx.getInput(int.class);
                    value = ctx.callActivity(plusOneActivityName, value, int.class).await();
                    ctx.complete(value);
                })
                .addActivity(plusOneActivityName, ctx -> {
                    try {
                        // The orchestration is started but not completed within the orchestration completion timeout due the below activity delay
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return ctx.getInput(int.class) + 1;
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            client.createTaskHub(true);
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            assertThrows(TimeoutException.class, () -> client.waitForInstanceCompletion(instanceId, Duration.ofSeconds(2), false));
        }
    }

    @Test
    void activityFanOutWithException() throws TimeoutException {
        final String orchestratorName = "ActivityFanOut";
        final String activityName = "Divide";
        final int count = 10;
        final String exceptionMessage = "2 out of 6 tasks failed with an exception. See the exceptions list for details.";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    // Schedule each task to run in parallel
                    List<Task<Integer>> parallelTasks = IntStream.of(1,2,0,4,0,6)
                            .mapToObj(i -> ctx.callActivity(activityName, i, Integer.class))
                            .collect(Collectors.toList());

                    // Wait for all tasks to complete
                    try {
                        List<Integer> results = ctx.allOf(parallelTasks).await();
                        ctx.complete(results);
                    }catch (CompositeTaskFailedException e){
                        assertNotNull(e);
                        assertEquals(2, e.getExceptions().size());
                        assertEquals(TaskFailedException.class, e.getExceptions().get(0).getClass());
                        assertEquals(TaskFailedException.class, e.getExceptions().get(1).getClass());
                        // taskId in the exception below is based on parallelTasks input
                        assertEquals(getExceptionMessage(activityName, 2, "/ by zero"), e.getExceptions().get(0).getMessage());
                        assertEquals(getExceptionMessage(activityName, 4, "/ by zero"), e.getExceptions().get(1).getMessage());
                        throw e;
                    }
                })
                .addActivity(activityName, ctx -> count / ctx.getInput(Integer.class))
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            List<?> output = instance.readOutputAs(List.class);
            assertNull(output);

            FailureDetails details = instance.getFailureDetails();
            assertNotNull(details);
            assertEquals(exceptionMessage, details.getErrorMessage());
            assertEquals("com.microsoft.durabletask.CompositeTaskFailedException", details.getErrorType());
            assertNotNull(details.getStackTrace());
        }
    }
    private static String getExceptionMessage(String taskName, int expectedTaskId, String expectedExceptionMessage) {
        return String.format(
                "Task '%s' (#%d) failed with an unhandled exception: %s",
                taskName,
                expectedTaskId,
                expectedExceptionMessage);
    }

    @Test
    void thenApply() throws IOException, InterruptedException, TimeoutException {
        final String orchestratorName = "thenApplyActivity";
        final String activityName = "Echo";
        final String suffix = "-test";
        final String input = Instant.now().toString();
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    String activityInput = ctx.getInput(String.class);
                    String output = ctx.callActivity(activityName, activityInput, String.class).thenApply(s -> s + suffix).await();
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
            String expected = String.format("Hello, %s!%s", input, suffix);
            assertEquals(expected, output);
        }
    }

    @Test
    void externalEventThenAccept() throws InterruptedException, TimeoutException {
        final String orchestratorName = "continueAsNewWithExternalEvents";
        final String eventName = "MyEvent";
        final int expectedEventCount = 10;
        DurableTaskGrpcWorker worker = this.createWorkerBuilder().addOrchestrator(orchestratorName, ctx -> {
            int receivedEventCount = ctx.getInput(int.class);

            if (receivedEventCount < expectedEventCount) {
                ctx.waitForExternalEvent(eventName, int.class)
                        .thenAccept(s -> {
                            ctx.continueAsNew(receivedEventCount + 1);
                            return;
                        })
                        .await();
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
    void activityAllOf() throws IOException, TimeoutException {
        final String orchestratorName = "ActivityAllOf";
        final String activityName = "ToString";
        final String retryActivityName = "RetryToString";
        final int activityMiddle = 5;
        final int activityCount = 10;
        final AtomicBoolean throwException = new AtomicBoolean(true);
        final RetryPolicy retryPolicy = new RetryPolicy(2, Duration.ofSeconds(5));
        final TaskOptions taskOptions = new TaskOptions(retryPolicy);

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    List<Task<String>> parallelTasks = IntStream.range(0, activityMiddle * 2)
                            .mapToObj(i -> {
                                if (i < activityMiddle) {
                                    return ctx.callActivity(activityName, i, String.class);
                                } else {
                                    return ctx.callActivity(retryActivityName, i, taskOptions, String.class);
                                }
                            })
                            .collect(Collectors.toList());

                    // Wait for all tasks to complete, then sort and reverse the results
                    List<String> results = ctx.allOf(parallelTasks).await();
                    Collections.sort(results);
                    Collections.reverse(results);
                    ctx.complete(results);
                })
                .addActivity(activityName, ctx -> ctx.getInput(Object.class).toString())
                .addActivity(retryActivityName, ctx -> {
                    if (throwException.get()) {
                        throwException.compareAndSet(true, false);
                        throw new RuntimeException("test retry");
                    }
                    return ctx.getInput(Object.class).toString();
                })
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
    void activityAllOfException() throws IOException, TimeoutException {
        final String orchestratorName = "ActivityAllOf";
        final String activityName = "ToString";
        final String retryActivityName = "RetryToStringException";
        final String result = "test fail";
        final int activityMiddle = 5;
        final RetryPolicy retryPolicy = new RetryPolicy(2, Duration.ofSeconds(5));
        final TaskOptions taskOptions = new TaskOptions(retryPolicy);

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    List<Task<String>> parallelTasks = IntStream.range(0, activityMiddle * 2)
                            .mapToObj(i -> {
                                if (i < activityMiddle) {
                                    return ctx.callActivity(activityName, i, String.class);
                                } else {
                                    return ctx.callActivity(retryActivityName, i, taskOptions, String.class);
                                }
                            })
                            .collect(Collectors.toList());

                    // Wait for all tasks to complete, then sort and reverse the results
                    try {
                        List<String> results = null;
                        results = ctx.allOf(parallelTasks).await();
                        Collections.sort(results);
                        Collections.reverse(results);
                        ctx.complete(results);
                    } catch (CompositeTaskFailedException e) {
                        // only catch this type of exception to ensure the expected type of exception is thrown out.
                        for (Exception exception : e.getExceptions()) {
                            if (exception instanceof TaskFailedException) {
                                TaskFailedException taskFailedException = (TaskFailedException) exception;
                                System.out.println("Task: " + taskFailedException.getTaskName() +
                                        " Failed for cause: " + taskFailedException.getErrorDetails().getErrorMessage());
                            }
                        }
                    }
                    ctx.complete(result);
                })
                .addActivity(activityName, ctx -> ctx.getInput(Object.class).toString())
                .addActivity(retryActivityName, ctx -> {
                    // only throw exception
                    throw new RuntimeException("test retry");
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            String output = instance.readOutputAs(String.class);
            assertNotNull(output);
            assertEquals(String.class, output.getClass());
            assertEquals(result, output);
        }
    }

    @Test
    void activityAnyOf() throws IOException, TimeoutException {
        final String orchestratorName = "ActivityAnyOf";
        final String activityName = "ToString";
        final String retryActivityName = "RetryToString";
        final int activityMiddle = 5;
        final int activityCount = 10;
        final AtomicBoolean throwException = new AtomicBoolean(true);
        final RetryPolicy retryPolicy = new RetryPolicy(2, Duration.ofSeconds(5));
        final TaskOptions taskOptions = new TaskOptions(retryPolicy);

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    List<Task<?>> parallelTasks = IntStream.range(0, activityMiddle * 2)
                            .mapToObj(i -> {
                                if (i < activityMiddle) {
                                    return ctx.callActivity(activityName, i, String.class);
                                } else {
                                    return ctx.callActivity(retryActivityName, i, taskOptions, String.class);
                                }
                            })
                            .collect(Collectors.toList());

                    String results = (String) ctx.anyOf(parallelTasks).await().await();
                    ctx.complete(results);
                })
                .addActivity(activityName, ctx -> ctx.getInput(Object.class).toString())
                .addActivity(retryActivityName, ctx -> {
                    if (throwException.get()) {
                        throwException.compareAndSet(true, false);
                        throw new RuntimeException("test retry");
                    }
                    return ctx.getInput(Object.class).toString();
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());

            String output = instance.readOutputAs(String.class);
            assertNotNull(output);
            assertTrue(Integer.parseInt(output) >= 0 && Integer.parseInt(output) < activityCount);
        }
    }

    @Test
    public void newUUIDTest() {
        String orchestratorName = "test-new-uuid";
        String echoActivityName = "Echo";
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    // Test 1: Ensure two consequiteively created GUIDs are not unique
                    UUID currentUUID0 = ctx.newUUID();
                    UUID currentUUID1 = ctx.newUUID();
                    if (currentUUID0.equals(currentUUID1)) {
                        ctx.complete(false);
                    }

                    // Test 2: Ensure that the same GUID values are created on each replay
                    UUID originalUUID1 = ctx.callActivity(echoActivityName, currentUUID1, UUID.class).await();
                    if (!currentUUID1.equals(originalUUID1)) {
                        ctx.complete(false);
                    }

                    // Test 3: Ensure that the same UUID values are created on each replay even after an await
                    UUID currentUUID2 = ctx.newUUID();
                    UUID originalUUID2 = ctx.callActivity(echoActivityName, currentUUID2, UUID.class).await();
                    if (!currentUUID2.equals(originalUUID2)) {
                        ctx.complete(false);
                    }

                    // Test 4: Finish confirming that every generated UUID is unique
                    if (currentUUID1.equals(currentUUID2)) ctx.complete(false);
                    else ctx.complete(true);
                })
                .addActivity(echoActivityName, ctx -> ctx.getInput(UUID.class))
                .buildAndStart();
        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();

        try(worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
            assertTrue(instance.readOutputAs(boolean.class));
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

    }
}