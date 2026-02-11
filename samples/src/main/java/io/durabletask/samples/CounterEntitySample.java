// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Sample demonstrating a simple counter durable entity.
 * <p>
 * The counter entity supports three operations:
 * <ul>
 *   <li>{@code add} — adds an integer amount to the counter</li>
 *   <li>{@code reset} — resets the counter to zero</li>
 *   <li>{@code get} — returns the current counter value</li>
 * </ul>
 * <p>
 * Usage: Run this sample with a Durable Task sidecar listening on localhost:4001.
 * <pre>
 *   docker run -d -p 4001:4001 durabletask-sidecar
 *   ./gradlew :samples:run -PmainClass=io.durabletask.samples.CounterEntitySample
 * </pre>
 */
final class CounterEntitySample {

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        // Build the worker with the counter entity registered
        DurableTaskGrpcWorker worker = new DurableTaskGrpcWorkerBuilder()
                .addEntity("Counter", CounterEntity::new)
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "CounterOrchestration"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            EntityInstanceId counterId = new EntityInstanceId("Counter", "myCounter");

                            // Signal entity to add 5
                            ctx.signalEntity(counterId, "add", 5);
                            // Signal entity to add 10
                            ctx.signalEntity(counterId, "add", 10);

                            // Call entity to get the current value
                            int value = ctx.callEntity(counterId, "get", Integer.class).await();

                            ctx.complete(value);
                        };
                    }
                })
                .build();

        worker.start();
        System.out.println("Worker started. Counter entity registered.");

        // Use the client to schedule an orchestration that interacts with the entity
        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();

        String instanceId = client.scheduleNewOrchestrationInstance("CounterOrchestration");
        System.out.printf("Started orchestration: %s%n", instanceId);

        OrchestrationMetadata result = client.waitForInstanceCompletion(
                instanceId, Duration.ofSeconds(30), true);
        System.out.printf("Orchestration completed: %s%n", result);
        System.out.printf("Counter value: %s%n", result.readOutputAs(Integer.class));

        // Query entity state directly
        EntityMetadata entityMetadata = client.getEntityMetadata(
                new EntityInstanceId("Counter", "myCounter"), true);
        if (entityMetadata != null) {
            System.out.printf("Entity state: %s%n", entityMetadata.readStateAs(Integer.class));
        }

        // Signal the entity to reset
        client.signalEntity(new EntityInstanceId("Counter", "myCounter"), "reset");
        System.out.println("Sent reset signal to counter entity.");

        worker.stop();
    }

    /**
     * A simple counter entity that stores an integer and supports add, reset, and get operations.
     */
    public static class CounterEntity extends TaskEntity<Integer> {

        public void add(int amount) {
            this.state += amount;
        }

        public void reset() {
            this.state = 0;
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
