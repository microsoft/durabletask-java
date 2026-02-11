// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TaskEntity} reflection-based dispatch.
 */
public class TaskEntityTest {

    // region Test entity classes

    /**
     * A simple counter entity for testing basic operations.
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

    /**
     * Entity that accepts a TaskEntityContext as a method parameter.
     */
    static class EntityWithContextParam extends TaskEntity<String> {
        public String info(TaskEntityContext context) {
            return "Entity ID: " + context.getId().toString();
        }

        @Override
        protected String initializeState(TaskEntityOperation operation) {
            return "";
        }

        @Override
        protected Class<String> getStateType() {
            return String.class;
        }
    }

    /**
     * Entity that accepts both input and context parameters.
     */
    static class EntityWithTwoParams extends TaskEntity<String> {
        public String greet(String name, TaskEntityContext context) {
            return "Hello, " + name + " from " + context.getId().getKey();
        }

        @Override
        protected String initializeState(TaskEntityOperation operation) {
            return "";
        }

        @Override
        protected Class<String> getStateType() {
            return String.class;
        }
    }

    /**
     * State class used for state dispatch testing.
     */
    static class MyState {
        private int value;

        public MyState() {
            this.value = 0;
        }

        public int getValue() {
            return this.value;
        }

        public void increment() {
            this.value++;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }

    /**
     * Entity that has no matching method but whose state type has the method (state dispatch).
     */
    static class StateDispatchEntity extends TaskEntity<MyState> {
        // No "increment" method on the entity itself — should dispatch to MyState.increment()

        @Override
        protected Class<MyState> getStateType() {
            return MyState.class;
        }
    }

    /**
     * Entity that throws during an operation.
     */
    static class ThrowingEntity extends TaskEntity<String> {
        public void fail() {
            throw new RuntimeException("Intentional failure");
        }

        @Override
        protected String initializeState(TaskEntityOperation operation) {
            return "initial";
        }

        @Override
        protected Class<String> getStateType() {
            return String.class;
        }
    }

    // endregion

    // region Helper methods

    private TaskEntityOperation createOperation(String operationName, Object input, String serializedState) {
        DataConverter converter = new JacksonDataConverter();
        String serializedInput = input != null ? converter.serialize(input) : null;

        // Create a minimal TaskEntityContext for testing
        EntityInstanceId entityId = new EntityInstanceId("TestEntity", "testKey");
        TaskEntityContext context = new TaskEntityContext() {
            @Override
            public EntityInstanceId getId() {
                return entityId;
            }

            @Override
            public void signalEntity(EntityInstanceId entityId, String operationName, Object input, SignalEntityOptions options) {
                // no-op for tests
            }

            @Override
            public String startNewOrchestration(String name, Object input, NewOrchestrationInstanceOptions options) {
                return "test-orchestration-id";
            }
        };

        TaskEntityState state = new TaskEntityState(converter, serializedState);

        return new TaskEntityOperation(operationName, serializedInput, context, state, converter);
    }

    private TaskEntityOperation createOperation(String operationName, Object input) {
        return createOperation(operationName, input, null);
    }

    private TaskEntityOperation createOperation(String operationName) {
        return createOperation(operationName, null, null);
    }

    // endregion

    // region Reflection dispatch tests

    @Test
    void reflectionDispatch_voidMethodNoArgs() throws Exception {
        CounterEntity entity = new CounterEntity();
        TaskEntityOperation op = createOperation("reset");
        entity.runAsync(op);
        assertEquals(0, entity.state);
    }

    @Test
    void reflectionDispatch_voidMethodWithArg() throws Exception {
        CounterEntity entity = new CounterEntity();
        DataConverter converter = new JacksonDataConverter();
        // Set initial state to 0
        TaskEntityOperation op = createOperation("add", 5);
        entity.runAsync(op);
        assertEquals(5, entity.state);
    }

    @Test
    void reflectionDispatch_methodWithReturnValue() throws Exception {
        CounterEntity entity = new CounterEntity();
        // Pre-load entity state to 42 and call "get"
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize(42);
        Object result = entity.runAsync(createOperation("get", null, serializedState));
        assertEquals(42, result);
    }

    @Test
    void reflectionDispatch_caseInsensitive() throws Exception {
        CounterEntity entity = new CounterEntity();
        DataConverter converter = new JacksonDataConverter();

        // Method is "add" but call with "ADD"
        TaskEntityOperation addOp = createOperation("ADD", 10);
        entity.runAsync(addOp);
        // After "add", state was saved to the operation's state
        String serializedState = addOp.getState().getSerializedState();
        assertEquals(10, entity.state);

        // Method is "get" but call with "Get" — carry state forward
        Object result = entity.runAsync(createOperation("Get", null, serializedState));
        assertEquals(10, result);
    }

    @Test
    void reflectionDispatch_methodWithContextParam() throws Exception {
        EntityWithContextParam entity = new EntityWithContextParam();
        Object result = entity.runAsync(createOperation("info"));
        assertNotNull(result);
        assertTrue(result.toString().contains("TestEntity"));
        assertTrue(result.toString().contains("testKey"));
    }

    @Test
    void reflectionDispatch_methodWithTwoParams() throws Exception {
        EntityWithTwoParams entity = new EntityWithTwoParams();
        Object result = entity.runAsync(createOperation("greet", "World"));
        assertEquals("Hello, World from testKey", result);
    }

    // endregion

    // region Implicit delete tests

    @Test
    void implicitDelete_deletesState() throws Exception {
        CounterEntity entity = new CounterEntity();
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize(42);

        TaskEntityOperation op = createOperation("delete", null, serializedState);
        entity.runAsync(op);

        assertFalse(op.getState().hasState());
        assertNull(entity.state);
    }

    @Test
    void implicitDelete_caseInsensitive() throws Exception {
        CounterEntity entity = new CounterEntity();
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize(42);

        TaskEntityOperation op = createOperation("DELETE", null, serializedState);
        entity.runAsync(op);

        assertFalse(op.getState().hasState());
    }

    // endregion

    // region State dispatch tests

    @Test
    void stateDispatch_delegatesToStateMethod() throws Exception {
        StateDispatchEntity entity = new StateDispatchEntity();
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize(new MyState());

        TaskEntityOperation op = createOperation("increment", null, serializedState);
        entity.runAsync(op);

        // State should have been incremented
        assertEquals(1, entity.state.getValue());
    }

    // endregion

    // region Error handling tests

    @Test
    void unknownOperation_throwsException() {
        CounterEntity entity = new CounterEntity();
        assertThrows(UnsupportedOperationException.class, () -> {
            entity.runAsync(createOperation("nonExistentOperation"));
        });
    }

    @Test
    void throwingOperation_propagatesException() {
        ThrowingEntity entity = new ThrowingEntity();
        RuntimeException ex = assertThrows(RuntimeException.class, () -> {
            entity.runAsync(createOperation("fail"));
        });
        assertEquals("Intentional failure", ex.getMessage());
    }

    // endregion

    // region State initialization tests

    @Test
    void stateInitialization_defaultInitializer() throws Exception {
        CounterEntity entity = new CounterEntity();
        TaskEntityOperation op = createOperation("get");
        Object result = entity.runAsync(op);
        // initializeState returns 0 for CounterEntity
        assertEquals(0, result);
    }

    @Test
    void stateInitialization_withExistingState() throws Exception {
        CounterEntity entity = new CounterEntity();
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize(99);

        TaskEntityOperation op = createOperation("get", null, serializedState);
        Object result = entity.runAsync(op);
        assertEquals(99, result);
    }

    @Test
    void statePersistence_stateIsSavedAfterOperation() throws Exception {
        CounterEntity entity = new CounterEntity();
        TaskEntityOperation op = createOperation("add", 5);
        entity.runAsync(op);

        // State should have been saved back
        assertTrue(op.getState().hasState());

        // Verify the saved state can be deserialized
        Integer savedState = op.getState().getState(Integer.class);
        assertEquals(5, savedState);
    }

    // endregion
}
