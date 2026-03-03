// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

/**
 * Sample demonstrating entity call timeouts and scheduled signals.
 * <p>
 * This sample shows two features:
 * <ul>
 *   <li><b>Call timeouts</b>: Using {@link CallEntityOptions#setTimeout(Duration)} to set a deadline
 *       on a {@code callEntity} call. If the entity doesn't respond in time, the orchestration
 *       receives a {@link TaskCanceledException}.</li>
 *   <li><b>Scheduled signals</b>: Using {@link SignalEntityOptions#setScheduledTime(Instant)} to
 *       schedule a signal for delivery at a future time (e.g., a reminder or expiration pattern).</li>
 * </ul>
 * <p>
 * Usage: Run this sample with a Durable Task sidecar listening on localhost:4001.
 * <pre>
 *   docker run -d -p 4001:4001 durabletask-sidecar
 *   ./gradlew :samples:run -PmainClass=io.durabletask.samples.EntityTimeoutSample
 * </pre>
 */
final class EntityTimeoutSample {

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        DurableTaskGrpcWorker worker = new DurableTaskGrpcWorkerBuilder()
                .addEntity("SlowCounter", SlowCounterEntity::new)
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "CallWithTimeout"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            EntityInstanceId counterId = new EntityInstanceId("SlowCounter", "myCounter");

                            // First: signal entity to set up initial state
                            ctx.signalEntity(counterId, "add", 100);

                            // Call entity with a generous timeout — should succeed
                            try {
                                CallEntityOptions options = new CallEntityOptions()
                                        .setTimeout(Duration.ofSeconds(30));
                                int value = ctx.callEntity(
                                        counterId, "get", null, Integer.class, options).await();
                                ctx.complete("Entity value: " + value);
                            } catch (TaskCanceledException e) {
                                ctx.complete("Call timed out: " + e.getMessage());
                            }
                        };
                    }
                })
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "CallWithShortTimeout"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            EntityInstanceId counterId = new EntityInstanceId("SlowCounter", "timeout-test");

                            // Call entity with a very short timeout — demonstrates timeout handling
                            try {
                                CallEntityOptions options = new CallEntityOptions()
                                        .setTimeout(Duration.ofMillis(1));
                                int value = ctx.callEntity(
                                        counterId, "get", null, Integer.class, options).await();
                                ctx.complete("Got value: " + value);
                            } catch (TaskCanceledException e) {
                                // The orchestration can gracefully handle the timeout
                                ctx.complete("Handled timeout gracefully: " + e.getMessage());
                            }
                        };
                    }
                })
                .build();

        worker.start();
        System.out.println("Worker started. SlowCounter entity registered.");

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();

        // --- Demo 1: Successful call with generous timeout ---
        System.out.println("\n--- Demo 1: callEntity with generous timeout ---");
        String instanceId1 = client.scheduleNewOrchestrationInstance("CallWithTimeout");
        OrchestrationMetadata result1 = client.waitForInstanceCompletion(
                instanceId1, Duration.ofSeconds(60), true);
        System.out.printf("Result: %s%n", result1.readOutputAs(String.class));

        // --- Demo 2: Call with very short timeout ---
        System.out.println("\n--- Demo 2: callEntity with short timeout ---");
        String instanceId2 = client.scheduleNewOrchestrationInstance("CallWithShortTimeout");
        OrchestrationMetadata result2 = client.waitForInstanceCompletion(
                instanceId2, Duration.ofSeconds(30), true);
        System.out.printf("Result: %s%n", result2.readOutputAs(String.class));

        // --- Demo 3: Scheduled signal ---
        System.out.println("\n--- Demo 3: Scheduled signal (5 seconds in the future) ---");
        EntityInstanceId counterId = new EntityInstanceId("SlowCounter", "scheduled-test");

        // Initialize the counter
        client.getEntities().signalEntity(counterId, "add", 50);
        Thread.sleep(2000);

        // Schedule a signal to add 25 more, delivered 5 seconds from now
        Instant scheduledTime = Instant.now().plusSeconds(5);
        SignalEntityOptions signalOptions = new SignalEntityOptions()
                .setScheduledTime(scheduledTime);
        client.getEntities().signalEntity(counterId, "add", 25, signalOptions);
        System.out.printf("Scheduled 'add 25' signal for %s%n", scheduledTime);

        // Check state before the scheduled signal is delivered
        EntityMetadata beforeMeta = client.getEntities().getEntityMetadata(counterId, true);
        if (beforeMeta != null) {
            System.out.printf("State before scheduled signal: %d%n",
                    beforeMeta.readStateAs(Integer.class));
        }

        // Wait for the scheduled signal to be delivered
        System.out.println("Waiting for scheduled signal delivery...");
        Thread.sleep(7000);

        // Check state after
        EntityMetadata afterMeta = client.getEntities().getEntityMetadata(counterId, true);
        if (afterMeta != null) {
            System.out.printf("State after scheduled signal: %d%n",
                    afterMeta.readStateAs(Integer.class));
        }

        worker.stop();
    }

    /**
     * A counter entity used for timeout and scheduled signal demonstrations.
     */
    public static class SlowCounterEntity extends TaskEntity<Integer> {

        public void add(int amount) {
            this.state += amount;
        }

        public int get() {
            return this.state;
        }

        @Override
        protected Integer initializeState(TaskEntityOperation operation) {
            return 0;
        }

        @Override
        protected Class<Integer> getStateType() {
            return Integer.class;
        }
    }
}
