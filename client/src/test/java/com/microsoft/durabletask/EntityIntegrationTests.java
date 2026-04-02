// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
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
    private static final Duration ENTITY_ORCHESTRATION_TIMEOUT = Duration.ofSeconds(30);

    // region Test entity classes

    /**
     * A simple counter entity for integration testing.
     */
    static class CounterEntity extends AbstractTaskEntity<Integer> {
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
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-signal-" + UUID.randomUUID());

        this.createWorkerBuilder()
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        // Signal the entity to add 10
        client.getEntities().signalEntity(entityId, "add", 10);

        // Wait for the entity to process the signal
        Thread.sleep(5000);

        // Query entity state
        EntityMetadata metadata = client.getEntities().getEntityMetadata(entityId, true);
        assertNotNull(metadata, "Entity metadata should not be null");

        Integer state = metadata.readStateAs(Integer.class);
        assertNotNull(state, "Entity state should not be null");
        assertEquals(10, state, "Entity state should be 10 after adding 10");
    }

    @Test
    void signalEntityMultipleSignals() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-multi-" + UUID.randomUUID());

        this.createWorkerBuilder()
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        // Send multiple signals
        client.getEntities().signalEntity(entityId, "add", 5);
        client.getEntities().signalEntity(entityId, "add", 3);
        client.getEntities().signalEntity(entityId, "add", 2);

        // Wait for all signals to be processed
        Thread.sleep(8000);

        EntityMetadata metadata = client.getEntities().getEntityMetadata(entityId, true);
        assertNotNull(metadata);

        Integer state = metadata.readStateAs(Integer.class);
        assertNotNull(state);
        assertEquals(10, state, "Entity state should be 10 after adding 5+3+2");
    }

    @Test
    void signalEntityResetAndGet() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-reset-" + UUID.randomUUID());

        this.createWorkerBuilder()
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        // Add, then reset
        client.getEntities().signalEntity(entityId, "add", 42);
        Thread.sleep(3000);
        client.getEntities().signalEntity(entityId, "reset");
        Thread.sleep(5000);

        EntityMetadata metadata = client.getEntities().getEntityMetadata(entityId, true);
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
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-orch-call-" + UUID.randomUUID());

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
            instanceId, ENTITY_ORCHESTRATION_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(42, instance.readOutputAs(int.class));
    }

    @Test
    void orchestrationSignalsEntityFireAndForget() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        final String orchestratorName = "SignalEntityOrchestration";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-orch-signal-" + UUID.randomUUID());

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
            instanceId, ENTITY_ORCHESTRATION_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals("signaled", instance.readOutputAs(String.class));

        // Wait for the signal to be processed
        Thread.sleep(5000);

        // Verify entity state was updated
        EntityMetadata metadata = client.getEntities().getEntityMetadata(entityId, true);
        assertNotNull(metadata);
        Integer state = metadata.readStateAs(Integer.class);
        assertNotNull(state);
        assertEquals(100, state);
    }

    // endregion

    // region Case-insensitive entity name tests

    @Test
    void signalEntity_caseInsensitiveName_entityProcessesSignal() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        // Use mixed case for the entity ID — should still work since names are lowercased
        EntityInstanceId entityId = new EntityInstanceId("COUNTER", "counter-case-" + UUID.randomUUID());

        this.createWorkerBuilder()
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        client.getEntities().signalEntity(entityId, "add", 25);
        Thread.sleep(5000);

        EntityMetadata metadata = client.getEntities().getEntityMetadata(entityId, true);
        assertNotNull(metadata, "Entity metadata should not be null");

        Integer state = metadata.readStateAs(Integer.class);
        assertNotNull(state, "Entity state should not be null");
        assertEquals(25, state, "Entity state should be 25 after adding 25");
    }

    // endregion

    // region Lock entities + getLockedEntities tests

    @Test
    void orchestration_lockAndCallEntity_succeeds() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        final String orchestratorName = "LockEntityOrchestration";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-lock-" + UUID.randomUUID());

        this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    AutoCloseable lock = ctx.lockEntities(Arrays.asList(entityId)).await();
                    // Verify we are in a critical section with locked entities
                    assertTrue(ctx.isInCriticalSection());
                    assertFalse(ctx.getLockedEntities().isEmpty());
                    // Call the locked entity
                    ctx.signalEntity(entityId, "add", 10);
                    try {
                        lock.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    ctx.complete("lock-success");
                })
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, ENTITY_ORCHESTRATION_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals("lock-success", instance.readOutputAs(String.class));
    }

    // endregion

    // region Typed entity proxy integration tests

    /**
     * Interface for typed proxy interaction with CounterEntity.
     */
    public interface ICounter {
        void add(int amount);
        void reset();
        Task<Integer> get();
    }

    @Test
    void typedProxy_signalAndCallEntity() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        final String orchestratorName = "TypedProxyOrchestration";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-proxy-" + UUID.randomUUID());

        this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    // Use the typed entity proxy
                    ICounter counter = ctx.createEntityProxy(entityId, ICounter.class);

                    // Void methods → signals
                    counter.add(10);
                    counter.add(20);
                    counter.add(12);

                    // Wait for signals to process, then call to get value
                    ctx.createTimer(Duration.ofSeconds(3)).await();
                    int value = counter.get().await();
                    ctx.complete(value);
                })
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, ENTITY_ORCHESTRATION_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(42, instance.readOutputAs(int.class));
    }

    @Test
    void typedProxy_viaEntitiesFeature() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        final String orchestratorName = "TypedProxyFeatureOrchestration";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-pf-" + UUID.randomUUID());

        this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    // Use the entities() feature to create a proxy
                    ICounter counter = ctx.entities().createProxy(entityId, ICounter.class);

                    counter.add(100);
                    ctx.createTimer(Duration.ofSeconds(3)).await();
                    int value = counter.get().await();
                    ctx.complete(value);
                })
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, ENTITY_ORCHESTRATION_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        assertEquals(100, instance.readOutputAs(int.class));
    }

    // endregion

    // region Re-entrant entity dispatch integration tests

    /**
     * Entity that uses re-entrant dispatch to compose operations.
     */
    static class BonusCounterEntity extends AbstractTaskEntity<Integer> {
        public void add(int amount) {
            this.state += amount;
        }

        public void addWithBonus(int amount) {
            dispatch("add", amount);           // main add
            dispatch("add", amount / 10);      // 10% bonus
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

    @Test
    void reentrantDispatch_composesOperations() throws TimeoutException, InterruptedException {
        final String entityName = "BonusCounter";
        final String orchestratorName = "ReentrantDispatchOrchestration";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "bonus-counter-" + UUID.randomUUID());

        this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    // Signal entity to add with bonus
                    ctx.signalEntity(entityId, "addWithBonus", 100);

                    // Wait for signal to process, then get value
                    ctx.createTimer(Duration.ofSeconds(3)).await();
                    int value = ctx.callEntity(entityId, "get", Integer.class).await();
                    ctx.complete(value);
                })
                .addEntity(entityName, BonusCounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, ENTITY_ORCHESTRATION_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
        // 100 + 10% bonus = 110
        assertEquals(110, instance.readOutputAs(int.class));
    }

    // endregion

    // region ContinueAsNew guard in critical section tests

    @Test
    void continueAsNew_insideCriticalSection_throwsIllegalStateException() throws TimeoutException, InterruptedException {
        final String entityName = "Counter";
        final String orchestratorName = "ContinueAsNewInCriticalSectionOrchestration";
        EntityInstanceId entityId = new EntityInstanceId(entityName, "counter-cas-" + UUID.randomUUID());

        this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    AutoCloseable lock = ctx.lockEntities(Arrays.asList(entityId)).await();
                    assertTrue(ctx.isInCriticalSection());
                    // This should throw IllegalStateException
                    ctx.continueAsNew(null);
                })
                .addEntity(entityName, CounterEntity::new)
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();

        String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName);
        OrchestrationMetadata instance = client.waitForInstanceCompletion(
            instanceId, ENTITY_ORCHESTRATION_TIMEOUT, true);

        assertNotNull(instance);
        assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

        FailureDetails details = instance.getFailureDetails();
        assertNotNull(details);
        assertEquals("java.lang.IllegalStateException", details.getErrorType());
        assertTrue(details.getErrorMessage().contains("Cannot continue-as-new while inside a critical section"));
    }

    // endregion
}
