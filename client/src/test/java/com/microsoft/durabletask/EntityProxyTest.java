// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EntityProxy} — typed entity proxy feature.
 */
public class EntityProxyTest {

    // region Test interfaces

    /**
     * Basic counter interface with signal (void) and call (Task) operations.
     */
    public interface ICounter {
        void add(int amount);
        void reset();
        Task<Integer> get();
    }

    /**
     * Interface with a parameterized call operation.
     */
    public interface IGreeter {
        Task<String> greet(String name);
    }

    /**
     * Interface with only void (signal) methods.
     */
    public interface ISignalOnly {
        void doSomething();
        void doSomethingWith(String data);
    }

    /**
     * Interface with an invalid return type (neither void nor Task).
     */
    public interface IInvalidReturn {
        String badMethod();
    }

    /**
     * Interface with too many parameters.
     */
    public interface ITooManyParams {
        void transfer(String from, String to, int amount);
    }

    /**
     * Not an interface — used to test validation.
     */
    public static class NotAnInterface {
    }

    // endregion

    // region Simple test Task implementation (package-private constructor accessible from same package)

    /**
     * Minimal concrete {@link Task} implementation for unit testing.
     */
    static class TestTask<V> extends Task<V> {
        TestTask(V value) {
            super(CompletableFuture.completedFuture(value));
        }

        @Override
        public V await() {
            try {
                return this.future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public <U> Task<U> thenApply(Function<V, U> fn) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Task<Void> thenAccept(Consumer<V> fn) {
            throw new UnsupportedOperationException();
        }
    }

    // endregion

    // region Recording context for verifying proxy behavior

    /**
     * Minimal {@link TaskOrchestrationContext} that records signal and call invocations.
     */
    static class RecordingContext implements TaskOrchestrationContext {
        final List<String> signals = new ArrayList<>();
        final List<String> calls = new ArrayList<>();

        // Captured state from the last signalEntity / callEntity call
        EntityInstanceId lastSignalEntityId;
        String lastSignalOp;
        Object lastSignalInput;

        EntityInstanceId lastCallEntityId;
        String lastCallOp;
        Object lastCallInput;
        Class<?> lastCallReturnType;

        @Override
        public String getName() { return "test"; }

        @Override
        public <V> V getInput(Class<V> targetType) { return null; }

        @Override
        public String getInstanceId() { return "test-instance"; }

        @Override
        public Instant getCurrentInstant() { return Instant.now(); }

        @Override
        public boolean getIsReplaying() { return false; }

        @Override
        public String getVersion() { return "1.0"; }

        @Override
        public <V> Task<List<V>> allOf(List<Task<V>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Task<Task<?>> anyOf(List<Task<?>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> Task<V> callActivity(String name, @Nullable Object input, @Nullable TaskOptions options, Class<V> returnType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> Task<V> callSubOrchestrator(String name, @Nullable Object input, @Nullable String instanceID, @Nullable TaskOptions options, Class<V> returnType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Task<Void> createTimer(Duration duration) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> Task<V> waitForExternalEvent(String name, Duration timeout, Class<V> dataType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void continueAsNew(Object input, boolean preserveUnprocessedEvents) { }

        @Override
        public void complete(Object output) { }

        @Override
        public void signalEntity(@Nonnull EntityInstanceId entityId, @Nonnull String operationName,
                                 @Nullable Object input, @Nullable SignalEntityOptions options) {
            this.lastSignalEntityId = entityId;
            this.lastSignalOp = operationName;
            this.lastSignalInput = input;
            this.signals.add(operationName);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> Task<V> callEntity(@Nonnull EntityInstanceId entityId, @Nonnull String operationName,
                                       @Nullable Object input, @Nonnull Class<V> returnType) {
            this.lastCallEntityId = entityId;
            this.lastCallOp = operationName;
            this.lastCallInput = input;
            this.lastCallReturnType = returnType;
            this.calls.add(operationName);
            // Return a completed task with a default value
            return new TestTask<>(null);
        }

        @Override
        public <V> Task<V> callEntity(@Nonnull EntityInstanceId entityId, @Nonnull String operationName,
                                       @Nullable Object input, @Nonnull Class<V> returnType,
                                       @Nullable CallEntityOptions options) {
            return callEntity(entityId, operationName, input, returnType);
        }

        @Override
        public Task<AutoCloseable> lockEntities(@Nonnull List<EntityInstanceId> entityIds) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isInCriticalSection() { return false; }

        @Override
        public List<EntityInstanceId> getLockedEntities() { return Collections.emptyList(); }

        @Override
        public void sendEvent(String instanceId, String eventName, Object eventData) { }

        @Override
        public void setCustomStatus(Object customStatus) { }

        @Override
        public void clearCustomStatus() { }
    }

    // endregion

    // region Proxy creation tests

    @Test
    void create_returnsProxyInstance() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        ICounter proxy = EntityProxy.create(ctx, entityId, ICounter.class);

        assertNotNull(proxy);
        assertTrue(proxy instanceof ICounter);
    }

    @Test
    void create_throwsForNonInterface() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        assertThrows(IllegalArgumentException.class,
                () -> EntityProxy.create(ctx, entityId, NotAnInterface.class));
    }

    @Test
    void create_throwsForNullArgs() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        assertThrows(IllegalArgumentException.class,
                () -> EntityProxy.create(null, entityId, ICounter.class));
        assertThrows(IllegalArgumentException.class,
                () -> EntityProxy.create(ctx, null, ICounter.class));
        assertThrows(IllegalArgumentException.class,
                () -> EntityProxy.create(ctx, entityId, null));
    }

    // endregion

    // region Void method → signalEntity tests

    @Test
    void voidMethod_noArgs_sendsSignal() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        ICounter proxy = EntityProxy.create(ctx, entityId, ICounter.class);

        proxy.reset();

        assertEquals(1, ctx.signals.size());
        assertEquals("reset", ctx.lastSignalOp);
        assertEquals(entityId, ctx.lastSignalEntityId);
        assertNull(ctx.lastSignalInput);
    }

    @Test
    void voidMethod_withArg_sendsSignalWithInput() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        ICounter proxy = EntityProxy.create(ctx, entityId, ICounter.class);

        proxy.add(42);

        assertEquals(1, ctx.signals.size());
        assertEquals("add", ctx.lastSignalOp);
        assertEquals(42, ctx.lastSignalInput);
    }

    @Test
    void voidMethod_multipleCallsRecorded() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        ICounter proxy = EntityProxy.create(ctx, entityId, ICounter.class);

        proxy.add(1);
        proxy.add(2);
        proxy.reset();

        assertEquals(3, ctx.signals.size());
        assertEquals(Arrays.asList("add", "add", "reset"), ctx.signals);
    }

    // endregion

    // region Task<V> method → callEntity tests

    @Test
    void taskMethod_noArgs_sendsCallWithReturnType() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        ICounter proxy = EntityProxy.create(ctx, entityId, ICounter.class);

        Task<Integer> task = proxy.get();

        assertNotNull(task);
        assertEquals(1, ctx.calls.size());
        assertEquals("get", ctx.lastCallOp);
        assertEquals(entityId, ctx.lastCallEntityId);
        assertNull(ctx.lastCallInput);
        assertEquals(Integer.class, ctx.lastCallReturnType);
    }

    @Test
    void taskMethod_withArg_sendsCallWithInput() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Greeter", "g1");
        IGreeter proxy = EntityProxy.create(ctx, entityId, IGreeter.class);

        Task<String> task = proxy.greet("World");

        assertNotNull(task);
        assertEquals(1, ctx.calls.size());
        assertEquals("greet", ctx.lastCallOp);
        assertEquals("World", ctx.lastCallInput);
        assertEquals(String.class, ctx.lastCallReturnType);
    }

    // endregion

    // region Invalid method signatures

    @Test
    void invalidReturnType_throws() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Test", "t1");
        IInvalidReturn proxy = EntityProxy.create(ctx, entityId, IInvalidReturn.class);

        assertThrows(UnsupportedOperationException.class, proxy::badMethod);
    }

    @Test
    void tooManyParams_throws() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Test", "t1");
        ITooManyParams proxy = EntityProxy.create(ctx, entityId, ITooManyParams.class);

        assertThrows(UnsupportedOperationException.class,
                () -> proxy.transfer("a", "b", 100));
    }

    // endregion

    // region Object method handling

    @Test
    void toString_returnsDescription() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        ICounter proxy = EntityProxy.create(ctx, entityId, ICounter.class);

        String str = proxy.toString();

        assertTrue(str.contains("EntityProxy"));
        assertTrue(str.contains("counter"));  // entity names are lowercased
        assertTrue(str.contains("c1"));
    }

    @Test
    void hashCode_returnsEntityIdHash() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        ICounter proxy = EntityProxy.create(ctx, entityId, ICounter.class);

        assertEquals(entityId.hashCode(), proxy.hashCode());
    }

    @Test
    void equals_sameProxy_returnsTrue() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        ICounter proxy = EntityProxy.create(ctx, entityId, ICounter.class);

        assertTrue(proxy.equals(proxy));
    }

    @Test
    void equals_sameEntityId_returnsTrue() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        ICounter proxy1 = EntityProxy.create(ctx, entityId, ICounter.class);
        ICounter proxy2 = EntityProxy.create(ctx, entityId, ICounter.class);

        assertTrue(proxy1.equals(proxy2));
    }

    @Test
    void equals_differentEntityId_returnsFalse() {
        RecordingContext ctx = new RecordingContext();
        ICounter proxy1 = EntityProxy.create(ctx, new EntityInstanceId("Counter", "c1"), ICounter.class);
        ICounter proxy2 = EntityProxy.create(ctx, new EntityInstanceId("Counter", "c2"), ICounter.class);

        assertFalse(proxy1.equals(proxy2));
    }

    @Test
    void equals_null_returnsFalse() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");
        ICounter proxy = EntityProxy.create(ctx, entityId, ICounter.class);

        assertFalse(proxy.equals(null));
    }

    // endregion

    // region Context convenience method tests

    @Test
    void createEntityProxy_onContext_createsProxy() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        ICounter proxy = ctx.createEntityProxy(entityId, ICounter.class);

        assertNotNull(proxy);
        proxy.add(10);
        assertEquals("add", ctx.lastSignalOp);
    }

    @Test
    void createProxy_onEntityFeature_createsProxy() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        ICounter proxy = ctx.entities().createProxy(entityId, ICounter.class);

        assertNotNull(proxy);
        proxy.add(10);
        assertEquals("add", ctx.lastSignalOp);
    }

    // endregion

    // region Signal-only interface tests

    @Test
    void signalOnlyInterface_allMethodsSendSignals() {
        RecordingContext ctx = new RecordingContext();
        EntityInstanceId entityId = new EntityInstanceId("Worker", "w1");
        ISignalOnly proxy = EntityProxy.create(ctx, entityId, ISignalOnly.class);

        proxy.doSomething();
        proxy.doSomethingWith("data");

        assertEquals(2, ctx.signals.size());
        assertEquals(0, ctx.calls.size());
    }

    // endregion
}
