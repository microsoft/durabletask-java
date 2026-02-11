// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for durable entity features.
 * <p>
 * These tests require a sidecar process running on {@code localhost:4001}.
 * They exercise the complete entity workflow: client signaling, entity execution,
 * state persistence, and orchestration-entity interaction.
 */
@Tag("integration")
public class EntityIntegrationTests extends IntegrationTestBase {

    // region Test entity classes

    /**
     * A simple counter entity for integration testing.
     */
    static class CounterEntity extends TaskEntity<Integer> {
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

    // endregion

    // region Signal entity + state query tests

    @Test
    void signalEntityAndGetState() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-signal-test");

        this.createWorkerBuilder()
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        // Signal the entity to add 10
        client.signalEntity(entityId, "add", 10);

        // Wait for the entity to process the signal
        Thread.sleep(5000);

        // Query entity state
        EntityMetadata metadata = client.getEntityMetadata(entityId, true);
        assertNotNull(metadata, "Entity metadata should not be null");

        Integer state = metadata.readStateAs(Integer.class);
        assertNotNull(state, "Entity state should not be null");
        assertEquals(10, state, "Entity state should be 10 after adding 10");
    }

    @Test
    void signalEntityMultipleSignals() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-multi-signal");

        this.createWorkerBuilder()
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        // Send multiple signals
        client.signalEntity(entityId, "add", 5);
        client.signalEntity(entityId, "add", 3);
        client.signalEntity(entityId, "add", 2);

        // Wait for all signals to be processed
        Thread.sleep(8000);

        EntityMetadata metadata = client.getEntityMetadata(entityId, true);
        assertNotNull(metadata);

        Integer state = metadata.readStateAs(Integer.class);
        assertNotNull(state);
        assertEquals(10, state, "Entity state should be 10 after adding 5+3+2");
    }

    @Test
    void signalEntityResetAndGet() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-reset");

        this.createWorkerBuilder()
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        // Add, then reset
        client.signalEntity(entityId, "add", 42);
        Thread.sleep(3000);
        client.signalEntity(entityId, "reset");
        Thread.sleep(5000);

        EntityMetadata metadata = client.getEntityMetadata(entityId, true);
        assertNotNull(metadata);

        Integer state = metadata.readStateAs(Integer.class);
        assertNotNull(state);
        assertEquals(0, state, "Entity state should be 0 after reset");
    }

    // endregion

    // region Orchestration ↔ entity tests

    @Test
    void orchestrationCallsEntity() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        final String orchestratorName = "CallEntityOrchestration";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-orch-call");

        this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    // Signal the entity to set up some state
                    ctx.signalEntity(entityId, "add", 42);
                    // Wait for the signal to be processed
                    ctx.createTimer(Duration.ofSeconds(3)).await();
                    // Then call to get the value
                    int value = ctx.callEntity(entityId, "get", null, int.class).await();
                    ctx.complete(value);
                })
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(42, instance.readOutputAs(int.class));
    }

    @Test
    void orchestrationSignalsEntityFireAndForget() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        final String orchestratorName = "SignalEntityOrchestration";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-orch-signal");

        this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    ctx.signalEntity(entityId, "add", 100);
                    ctx.complete("signaled");
                })
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
                instanceId, defaultTimeout, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals("signaled", instance.readOutputAs(String.class));

        // Wait for the signal to be processed
        Thread.sleep(5000);

        // Verify entity state was updated
        EntityMetadata metadata = client.getEntityMetadata(entityId, true);
        assertNotNull(metadata);
        Integer state = metadata.readStateAs(Integer.class);
        assertNotNull(state);
        assertEquals(100, state);
    }

    // endregion
}
