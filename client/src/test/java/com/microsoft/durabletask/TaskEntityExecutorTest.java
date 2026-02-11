// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TaskEntityExecutor}.
 * <p>
 * Tests construct {@link EntityBatchRequest} proto objects directly and assert on
 * the returned {@link EntityBatchResult}, following the same pattern as
 * {@link TaskOrchestrationExecutorTest}.
 */
public class TaskEntityExecutorTest {

    private static final Logger logger = Logger.getLogger(TaskEntityExecutorTest.class.getName());
    private static final DataConverter dataConverter = new JacksonDataConverter();

    // region Test entity implementations

    /**
     * Simple counter entity for testing.
     */
    static class TestCounterEntity extends TaskEntity<Integer> {
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

    /**
     * Entity that always throws (for failure testing).
     */
    static class FailingEntity implements ITaskEntity {
        @Override
        public Object runAsync(TaskEntityOperation operation) throws Exception {
            throw new RuntimeException("Intentional failure: " + operation.getName());
        }
    }

    /**
     * Entity that signals another entity during execution (for action testing).
     */
    static class SignalingEntity implements ITaskEntity {
        @Override
        public Object runAsync(TaskEntityOperation operation) throws Exception {
            if ("signalOther".equals(operation.getName())) {
                EntityInstanceId targetId = new EntityInstanceId("Counter", "target1");
                operation.getContext().signalEntity(targetId, "add", 10);
                return "signaled";
            }
            return null;
        }
    }

    /**
     * Entity that starts an orchestration during execution.
     */
    static class OrchestrationStartingEntity implements ITaskEntity {
        @Override
        public Object runAsync(TaskEntityOperation operation) throws Exception {
            if ("startOrch".equals(operation.getName())) {
                String orchId = operation.getContext().startNewOrchestration("MyOrchestration", "orchInput");
                return orchId;
            }
            return null;
        }
    }

    /**
     * Entity that conditionally fails (first op succeeds, second fails).
     */
    static class ConditionalFailEntity implements ITaskEntity {
        private int callCount = 0;

        @Override
        public Object runAsync(TaskEntityOperation operation) throws Exception {
            callCount++;
            if ("failOnSecond".equals(operation.getName()) && callCount == 2) {
                throw new RuntimeException("Second call failed");
            }
            // Modify state to test rollback
            operation.getState().setState("state-after-op-" + callCount);
            return "result-" + callCount;
        }
    }

    // endregion

    // region Helper methods

    private TaskEntityExecutor createExecutor(String entityName, TaskEntityFactory factory) {
        HashMap<String, TaskEntityFactory> factories = new HashMap<>();
        factories.put(entityName, factory);
        return new TaskEntityExecutor(factories, dataConverter, logger);
    }

    private OperationRequest buildOperationRequest(String operationName, Object input, String requestId) {
        OperationRequest.Builder builder = OperationRequest.newBuilder()
                .setOperation(operationName)
                .setRequestId(requestId != null ? requestId : "req-" + operationName);
        if (input != null) {
            builder.setInput(StringValue.of(dataConverter.serialize(input)));
        }
        return builder.build();
    }

    private OperationRequest buildOperationRequest(String operationName, Object input) {
        return buildOperationRequest(operationName, input, null);
    }

    private OperationRequest buildOperationRequest(String operationName) {
        return buildOperationRequest(operationName, null, null);
    }

    private EntityBatchRequest buildBatchRequest(String entityName, String entityKey, String entityState,
                                                  OperationRequest... operations) {
        EntityBatchRequest.Builder builder = EntityBatchRequest.newBuilder()
                .setInstanceId("@" + entityName + "@" + entityKey);
        if (entityState != null) {
            builder.setEntityState(StringValue.of(entityState));
        }
        for (OperationRequest op : operations) {
            builder.addOperations(op);
        }
        return builder.build();
    }

    // endregion

    // region Single operation tests

    @Test
    void execute_singleSuccessfulOperation_returnsSuccess() {
        TaskEntityExecutor executor = createExecutor("Counter", TestCounterEntity::new);

        EntityBatchRequest request = buildBatchRequest("Counter", "c1",
                dataConverter.serialize(10),
                buildOperationRequest("add", 5));

        EntityBatchResult result = executor.execute(request);

        assertEquals(1, result.getResultsCount());
        assertTrue(result.getResults(0).hasSuccess());

        // Result has no explicit output (void method)
        assertFalse(result.getResults(0).getSuccess().hasResult());

        // Final state should be 15 (10 + 5)
        assertTrue(result.hasEntityState());
        assertEquals("15", result.getEntityState().getValue());
    }

    @Test
    void execute_operationWithReturnValue_returnsSerializedResult() {
        TaskEntityExecutor executor = createExecutor("Counter", TestCounterEntity::new);

        EntityBatchRequest request = buildBatchRequest("Counter", "c1",
                dataConverter.serialize(42),
                buildOperationRequest("get"));

        EntityBatchResult result = executor.execute(request);

        assertEquals(1, result.getResultsCount());
        assertTrue(result.getResults(0).hasSuccess());
        assertTrue(result.getResults(0).getSuccess().hasResult());
        assertEquals("42", result.getResults(0).getSuccess().getResult().getValue());
    }

    @Test
    void execute_operationWithNoExistingState_initializesState() {
        TaskEntityExecutor executor = createExecutor("Counter", TestCounterEntity::new);

        // No entity state provided — should call initializeState() which returns 0
        EntityBatchRequest request = buildBatchRequest("Counter", "c1", null,
                buildOperationRequest("add", 7));

        EntityBatchResult result = executor.execute(request);

        assertEquals(1, result.getResultsCount());
        assertTrue(result.getResults(0).hasSuccess());

        // State should be 0 + 7 = 7
        assertTrue(result.hasEntityState());
        assertEquals("7", result.getEntityState().getValue());
    }

    @Test
    void execute_operationFails_returnsFailure() {
        TaskEntityExecutor executor = createExecutor("Failing", FailingEntity::new);

        EntityBatchRequest request = buildBatchRequest("Failing", "f1", null,
                buildOperationRequest("anyOp"));

        EntityBatchResult result = executor.execute(request);

        assertEquals(1, result.getResultsCount());
        assertTrue(result.getResults(0).hasFailure());

        TaskFailureDetails failure = result.getResults(0).getFailure().getFailureDetails();
        assertEquals("java.lang.RuntimeException", failure.getErrorType());
        assertTrue(failure.getErrorMessage().contains("Intentional failure"));
    }

    // endregion

    // region Batch (multi-operation) tests

    @Test
    void execute_multipleSuccessfulOperations_allSucceed() {
        TaskEntityExecutor executor = createExecutor("Counter", TestCounterEntity::new);

        EntityBatchRequest request = buildBatchRequest("Counter", "c1",
                dataConverter.serialize(0),
                buildOperationRequest("add", 3),
                buildOperationRequest("add", 7),
                buildOperationRequest("get"));

        EntityBatchResult result = executor.execute(request);

        assertEquals(3, result.getResultsCount());

        // All should succeed
        assertTrue(result.getResults(0).hasSuccess());
        assertTrue(result.getResults(1).hasSuccess());
        assertTrue(result.getResults(2).hasSuccess());

        // Final get should return 10
        assertEquals("10", result.getResults(2).getSuccess().getResult().getValue());

        // Final state should be 10
        assertEquals("10", result.getEntityState().getValue());
    }

    @Test
    void execute_batchWithFailure_rollbacksFailedOperation() {
        TaskEntityExecutor executor = createExecutor("Conditional", ConditionalFailEntity::new);

        EntityBatchRequest request = buildBatchRequest("Conditional", "key1", null,
                buildOperationRequest("failOnSecond"),
                buildOperationRequest("failOnSecond"));

        EntityBatchResult result = executor.execute(request);

        assertEquals(2, result.getResultsCount());

        // First operation succeeds
        assertTrue(result.getResults(0).hasSuccess());
        assertEquals("\"result-1\"", result.getResults(0).getSuccess().getResult().getValue());

        // Second operation fails
        assertTrue(result.getResults(1).hasFailure());
        assertTrue(result.getResults(1).getFailure().getFailureDetails().getErrorMessage().contains("Second call failed"));

        // State should be from the first successful operation (rolled back from second)
        assertTrue(result.hasEntityState());
        assertEquals("\"state-after-op-1\"", result.getEntityState().getValue());
    }

    @Test
    void execute_batchAfterFailure_continuesExecution() {
        // After a failed op, subsequent ops should still execute.
        // Using a fresh entity since ConditionalFailEntity tracks callCount.
        // Build a batch: op1 = fail, op2 = different entity that succeeds
        TaskEntityExecutor executor = createExecutor("Failing", FailingEntity::new);

        EntityBatchRequest request = buildBatchRequest("Failing", "key1", null,
                buildOperationRequest("op1"),
                buildOperationRequest("op2"));

        EntityBatchResult result = executor.execute(request);

        assertEquals(2, result.getResultsCount());
        // Both should fail since FailingEntity always fails
        assertTrue(result.getResults(0).hasFailure());
        assertTrue(result.getResults(1).hasFailure());
    }

    // endregion

    // region Unregistered entity tests

    @Test
    void execute_unregisteredEntity_returnsFailure() {
        TaskEntityExecutor executor = createExecutor("Counter", TestCounterEntity::new);

        // Request for a non-existent entity
        EntityBatchRequest request = buildBatchRequest("NonExistent", "key1", null,
                buildOperationRequest("op"));

        EntityBatchResult result = executor.execute(request);

        assertTrue(result.hasFailureDetails());
        assertEquals(IllegalStateException.class.getName(), result.getFailureDetails().getErrorType());
        assertTrue(result.getFailureDetails().getErrorMessage().contains("NonExistent"));
    }

    @Test
    void execute_caseInsensitiveEntityLookup_succeeds() {
        TaskEntityExecutor executor = createExecutor("Counter", TestCounterEntity::new);

        // Use different case for entity name in the instance ID
        EntityBatchRequest request = buildBatchRequest("counter", "c1",
                dataConverter.serialize(0),
                buildOperationRequest("add", 5));

        EntityBatchResult result = executor.execute(request);

        assertEquals(1, result.getResultsCount());
        assertTrue(result.getResults(0).hasSuccess());
    }

    // endregion

    // region Action (signal/orchestration) tests

    @Test
    void execute_entitySignalsOther_actionsIncluded() {
        TaskEntityExecutor executor = createExecutor("Signaler", SignalingEntity::new);

        EntityBatchRequest request = buildBatchRequest("Signaler", "s1", null,
                buildOperationRequest("signalOther"));

        EntityBatchResult result = executor.execute(request);

        assertEquals(1, result.getResultsCount());
        assertTrue(result.getResults(0).hasSuccess());
        assertEquals("\"signaled\"", result.getResults(0).getSuccess().getResult().getValue());

        // Should have one action: sendSignal
        assertEquals(1, result.getActionsCount());
        assertTrue(result.getActions(0).hasSendSignal());

        SendSignalAction signalAction = result.getActions(0).getSendSignal();
        assertEquals("@Counter@target1", signalAction.getInstanceId());
        assertEquals("add", signalAction.getName());
    }

    @Test
    void execute_entityStartsOrchestration_actionsIncluded() {
        TaskEntityExecutor executor = createExecutor("OrchStarter", OrchestrationStartingEntity::new);

        EntityBatchRequest request = buildBatchRequest("OrchStarter", "o1", null,
                buildOperationRequest("startOrch"));

        EntityBatchResult result = executor.execute(request);

        assertEquals(1, result.getResultsCount());
        assertTrue(result.getResults(0).hasSuccess());

        // Should have one action: startNewOrchestration
        assertEquals(1, result.getActionsCount());
        assertTrue(result.getActions(0).hasStartNewOrchestration());

        StartNewOrchestrationAction orchAction = result.getActions(0).getStartNewOrchestration();
        assertEquals("MyOrchestration", orchAction.getName());
    }

    @Test
    void execute_failedOperationRollsBackActions() {
        // Create an entity that signals another entity then fails
        TaskEntityFactory factory = () -> (ITaskEntity) operation -> {
            operation.getContext().signalEntity(
                    new EntityInstanceId("Other", "o1"), "op", null);
            throw new RuntimeException("fail after signal");
        };

        TaskEntityExecutor executor = createExecutor("FailAfterSignal", factory);

        EntityBatchRequest request = buildBatchRequest("FailAfterSignal", "key1", null,
                buildOperationRequest("op"));

        EntityBatchResult result = executor.execute(request);

        assertEquals(1, result.getResultsCount());
        assertTrue(result.getResults(0).hasFailure());

        // Actions should be empty because the operation failed and actions were rolled back
        assertEquals(0, result.getActionsCount());
    }

    // endregion

    // region Reset/delete tests

    @Test
    void execute_deleteOperation_deletesState() {
        TaskEntityExecutor executor = createExecutor("Counter", TestCounterEntity::new);

        EntityBatchRequest request = buildBatchRequest("Counter", "c1",
                dataConverter.serialize(42),
                buildOperationRequest("delete"));

        EntityBatchResult result = executor.execute(request);

        assertEquals(1, result.getResultsCount());
        assertTrue(result.getResults(0).hasSuccess());

        // State should be deleted (not set)
        assertFalse(result.hasEntityState());
    }

    @Test
    void execute_resetOperation_setsStateToZero() {
        TaskEntityExecutor executor = createExecutor("Counter", TestCounterEntity::new);

        EntityBatchRequest request = buildBatchRequest("Counter", "c1",
                dataConverter.serialize(42),
                buildOperationRequest("reset"),
                buildOperationRequest("get"));

        EntityBatchResult result = executor.execute(request);

        assertEquals(2, result.getResultsCount());
        assertTrue(result.getResults(0).hasSuccess());
        assertTrue(result.getResults(1).hasSuccess());
        assertEquals("0", result.getResults(1).getSuccess().getResult().getValue());
    }

    // endregion

    // region Timestamps tests

    @Test
    void execute_successResult_hasTimestamps() {
        TaskEntityExecutor executor = createExecutor("Counter", TestCounterEntity::new);

        EntityBatchRequest request = buildBatchRequest("Counter", "c1",
                dataConverter.serialize(0),
                buildOperationRequest("add", 1));

        EntityBatchResult result = executor.execute(request);

        OperationResultSuccess success = result.getResults(0).getSuccess();
        assertTrue(success.hasStartTimeUtc());
        assertTrue(success.hasEndTimeUtc());
        assertTrue(success.getEndTimeUtc().getSeconds() >= success.getStartTimeUtc().getSeconds());
    }

    @Test
    void execute_failureResult_hasTimestamps() {
        TaskEntityExecutor executor = createExecutor("Failing", FailingEntity::new);

        EntityBatchRequest request = buildBatchRequest("Failing", "f1", null,
                buildOperationRequest("op"));

        EntityBatchResult result = executor.execute(request);

        OperationResultFailure failure = result.getResults(0).getFailure();
        assertTrue(failure.hasStartTimeUtc());
        assertTrue(failure.hasEndTimeUtc());
    }

    // endregion

    // region Null factory result test

    @Test
    void execute_factoryReturnsNull_returnsFailure() {
        TaskEntityExecutor executor = createExecutor("NullEntity", () -> null);

        EntityBatchRequest request = buildBatchRequest("NullEntity", "n1", null,
                buildOperationRequest("op"));

        EntityBatchResult result = executor.execute(request);

        assertTrue(result.hasFailureDetails());
        assertTrue(result.getFailureDetails().getErrorMessage().contains("null"));
    }

    @Test
    void execute_factoryThrows_returnsFailure() {
        TaskEntityExecutor executor = createExecutor("ThrowFactory", () -> {
            throw new RuntimeException("Factory boom");
        });

        EntityBatchRequest request = buildBatchRequest("ThrowFactory", "t1", null,
                buildOperationRequest("op"));

        EntityBatchResult result = executor.execute(request);

        assertTrue(result.hasFailureDetails());
        assertTrue(result.getFailureDetails().getErrorMessage().contains("Factory boom"));
    }

    // endregion
}
