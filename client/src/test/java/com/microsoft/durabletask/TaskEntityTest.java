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
     * Explicitly enables state dispatch since the default is now {@code false}.
     */
    static class StateDispatchEntity extends TaskEntity<MyState> {
        // No "increment" method on the entity itself — should dispatch to MyState.increment()

        public StateDispatchEntity() {
            setAllowStateDispatch(true);
        }

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

    /**
     * Entity that disables state dispatch.
     */
    static class NoStateDispatchEntity extends TaskEntity<MyState> {
        public NoStateDispatchEntity() {
            setAllowStateDispatch(false);
        }

        @Override
        protected Class<MyState> getStateType() {
            return MyState.class;
        }
    }

    /**
     * Entity with overloaded methods (ambiguous match).
     */
    static class AmbiguousEntity extends TaskEntity<Integer> {
        public void add(int amount) {
            this.state += amount;
        }

        public void add(@SuppressWarnings("unused") String label) {
            // overloaded — should trigger ambiguous match error
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
        entity.run(op);
        assertEquals(0, entity.state);
    }

    @Test
    void reflectionDispatch_voidMethodWithArg() throws Exception {
        CounterEntity entity = new CounterEntity();
        // Set initial state to 0
        TaskEntityOperation op = createOperation("add", 5);
        entity.run(op);
        assertEquals(5, entity.state);
    }

    @Test
    void reflectionDispatch_methodWithReturnValue() throws Exception {
        CounterEntity entity = new CounterEntity();
        // Pre-load entity state to 42 and call "get"
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize(42);
        Object result = entity.run(createOperation("get", null, serializedState));
        assertEquals(42, result);
    }

    @Test
    void reflectionDispatch_caseInsensitive() throws Exception {
        CounterEntity entity = new CounterEntity();

        // Method is "add" but call with "ADD"
        TaskEntityOperation addOp = createOperation("ADD", 10);
        entity.run(addOp);
        // After "add", state was saved to the operation's state
        String serializedState = addOp.getState().getSerializedState();
        assertEquals(10, entity.state);

        // Method is "get" but call with "Get" — carry state forward
        Object result = entity.run(createOperation("Get", null, serializedState));
        assertEquals(10, result);
    }

    @Test
    void reflectionDispatch_methodWithContextParam() throws Exception {
        EntityWithContextParam entity = new EntityWithContextParam();
        Object result = entity.run(createOperation("info"));
        assertNotNull(result);
        assertTrue(result.toString().contains("testentity"));
        assertTrue(result.toString().contains("testKey"));
    }

    @Test
    void reflectionDispatch_methodWithTwoParams() throws Exception {
        EntityWithTwoParams entity = new EntityWithTwoParams();
        Object result = entity.run(createOperation("greet", "World"));
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
        entity.run(op);

        assertFalse(op.getState().hasState());
        assertNull(entity.state);
    }

    @Test
    void implicitDelete_caseInsensitive() throws Exception {
        CounterEntity entity = new CounterEntity();
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize(42);

        TaskEntityOperation op = createOperation("DELETE", null, serializedState);
        entity.run(op);

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
        entity.run(op);

        // State should have been incremented
        assertEquals(1, entity.state.getValue());
    }

    @Test
    void stateDispatch_disabledWithAllowStateDispatchFalse() {
        NoStateDispatchEntity entity = new NoStateDispatchEntity();
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize(new MyState());

        // "increment" exists on MyState but not on NoStateDispatchEntity.
        // With allowStateDispatch=false, it should throw UnsupportedOperationException.
        assertThrows(UnsupportedOperationException.class, () -> {
            entity.run(createOperation("increment", null, serializedState));
        });
    }

    @Test
    void stateDispatch_disabledByDefault() throws Exception {
        // Default is now false, matching the .NET SDK
        CounterEntity entity = new CounterEntity();
        assertFalse(entity.getAllowStateDispatch());
    }

    // endregion

    // region Ambiguous match tests

    @Test
    void ambiguousMatch_throwsIllegalStateException() {
        AmbiguousEntity entity = new AmbiguousEntity();
        // "add" has two overloads: add(int) and add(String) — should throw
        assertThrows(IllegalStateException.class, () -> {
            entity.run(createOperation("add", 5));
        });
    }

    // endregion

    // region Error handling tests

    @Test
    void unknownOperation_throwsException() {
        CounterEntity entity = new CounterEntity();
        assertThrows(UnsupportedOperationException.class, () -> {
            entity.run(createOperation("nonExistentOperation"));
        });
    }

    @Test
    void throwingOperation_propagatesException() {
        ThrowingEntity entity = new ThrowingEntity();
        RuntimeException ex = assertThrows(RuntimeException.class, () -> {
            entity.run(createOperation("fail"));
        });
        assertEquals("Intentional failure", ex.getMessage());
    }

    // endregion

    // region State initialization tests

    @Test
    void stateInitialization_defaultInitializer() throws Exception {
        CounterEntity entity = new CounterEntity();
        TaskEntityOperation op = createOperation("get");
        Object result = entity.run(op);
        // initializeState returns 0 for CounterEntity
        assertEquals(0, result);
    }

    @Test
    void stateInitialization_withExistingState() throws Exception {
        CounterEntity entity = new CounterEntity();
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize(99);

        TaskEntityOperation op = createOperation("get", null, serializedState);
        Object result = entity.run(op);
        assertEquals(99, result);
    }

    @Test
    void statePersistence_stateIsSavedAfterOperation() throws Exception {
        CounterEntity entity = new CounterEntity();
        TaskEntityOperation op = createOperation("add", 5);
        entity.run(op);

        // State should have been saved back
        assertTrue(op.getState().hasState());

        // Verify the saved state can be deserialized
        Integer savedState = op.getState().getState(Integer.class);
        assertEquals(5, savedState);
    }

    // endregion

    // region Re-entrant dispatch tests

    /**
     * Entity that uses dispatch() to compose operations re-entrantly.
     */
    static class BonusDepositEntity extends TaskEntity<Integer> {

        public void deposit(int amount) {
            this.state += amount;
        }

        public void depositWithBonus(int amount) {
            // Re-entrant: calls deposit() twice on the same entity
            dispatch("deposit", amount);           // main deposit
            dispatch("deposit", amount / 10);      // 10% bonus
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
     * Entity that uses dispatch() with a typed return value.
     */
    static class ComputeEntity extends TaskEntity<Integer> {

        public int double_value(int input) {
            return input * 2;
        }

        public int quadruple(int input) {
            // dispatch → double → then dispatch → double again
            int doubled = dispatch("double_value", input, int.class);
            return dispatch("double_value", doubled, int.class);
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
     * Entity that uses dispatch() to call an implicit "delete" operation.
     */
    static class SelfDeletingEntity extends TaskEntity<String> {

        public String resetAndDelete() {
            this.state = "resetting";
            dispatch("delete");
            return "deleted";
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

    /**
     * Entity that tests dispatch() to state-dispatched methods.
     */
    static class StateDispatchWithReentrancy extends TaskEntity<MyState> {

        public StateDispatchWithReentrancy() {
            setAllowStateDispatch(true);
        }

        public int getAndIncrement() {
            int before = this.state.getValue();
            dispatch("increment"); // dispatches to MyState.increment() via state dispatch
            return before;
        }

        @Override
        protected Class<MyState> getStateType() {
            return MyState.class;
        }
    }

    @Test
    void dispatch_basicReentrantCall() throws Exception {
        BonusDepositEntity entity = new BonusDepositEntity();
        TaskEntityOperation op = createOperation("depositWithBonus", 100);
        entity.run(op);

        // 100 + 10% bonus = 110
        assertEquals(110, entity.state);
    }

    @Test
    void dispatch_withTypedReturnValue() throws Exception {
        ComputeEntity entity = new ComputeEntity();
        TaskEntityOperation op = createOperation("quadruple", 5);
        Object result = entity.run(op);

        // 5 * 2 = 10, then 10 * 2 = 20
        assertEquals(20, result);
    }

    @Test
    void dispatch_implicitDelete() throws Exception {
        SelfDeletingEntity entity = new SelfDeletingEntity();
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize("existing");

        TaskEntityOperation op = createOperation("resetAndDelete", null, serializedState);
        Object result = entity.run(op);

        assertEquals("deleted", result);
        assertNull(entity.state);
    }

    @Test
    void dispatch_caseInsensitive() throws Exception {
        BonusDepositEntity entity = new BonusDepositEntity();
        // "deposit" is the method, but dispatch uses "DEPOSIT" internally — let's validate
        // by calling depositWithBonus which dispatches "deposit"
        TaskEntityOperation op = createOperation("DEPOSITWITHBONUS", 100);
        entity.run(op);
        assertEquals(110, entity.state);
    }

    @Test
    void dispatch_toStateDispatchedMethod() throws Exception {
        StateDispatchWithReentrancy entity = new StateDispatchWithReentrancy();
        DataConverter converter = new JacksonDataConverter();
        String serializedState = converter.serialize(new MyState());

        TaskEntityOperation op = createOperation("getAndIncrement", null, serializedState);
        Object result = entity.run(op);

        // Before increment was 0
        assertEquals(0, result);
        // State should now be 1 after dispatch("increment")
        assertEquals(1, entity.state.getValue());
    }

    @Test
    void dispatch_unknownOperation_throwsException() throws Exception {
        // First trigger run() to set the context on the entity, then test dispatch failure
        // We use a custom entity that dispatches an unknown operation
        TaskEntity<Void> failEntity = new TaskEntity<Void>() {
            public void bad() {
                dispatch("nonExistent");
            }

            @Override
            protected Class<Void> getStateType() {
                return null;
            }
        };

        assertThrows(UnsupportedOperationException.class, () -> {
            failEntity.run(createOperation("bad"));
        });
    }

    @Test
    void dispatch_outsideExecution_throwsIllegalState() {
        BonusDepositEntity entity = new BonusDepositEntity();
        // dispatch() called without run() first (no context set)
        assertThrows(IllegalStateException.class, () -> {
            entity.dispatch("deposit", 10);
        });
    }

    @Test
    void dispatch_noInputOverload() throws Exception {
        // Use a wrapper entity that dispatches "reset" with no input
        TaskEntity<Integer> resetDispatcher = new TaskEntity<Integer>() {
            public void doReset() {
                this.state = 42;
                dispatch("reset"); // reset sets state to 0
            }

            public void reset() {
                this.state = 0;
            }

            @Override
            protected Integer initializeState(TaskEntityOperation operation) {
                return 0;
            }

            @Override
            protected Class<Integer> getStateType() {
                return Integer.class;
            }
        };

        TaskEntityOperation op = createOperation("doReset");
        resetDispatcher.run(op);
        assertEquals(0, resetDispatcher.state);
    }

    // endregion
}
