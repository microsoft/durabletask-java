// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReplaySafeLoggerFactoryTest {

    @Mock
    private Logger mockLogger;

    // -----------------------------------------------------------------------
    // Constructor validation
    // -----------------------------------------------------------------------

    @Test
    void constructor_throwsOnNullContext() {
        assertThrows(IllegalArgumentException.class, () -> new ReplaySafeLoggerFactory(null));
    }

    // -----------------------------------------------------------------------
    // getLogger produces replay-safe loggers
    // -----------------------------------------------------------------------

    @Test
    void getLogger_returnsLoggerWithMatchingName() {
        ILoggerFactory underlying = mock(ILoggerFactory.class);
        when(underlying.getLogger("com.example.Test")).thenReturn(mockLogger);
        when(mockLogger.getName()).thenReturn("com.example.Test");

        TaskOrchestrationContext context = createMinimalContext(false, underlying);
        ReplaySafeLoggerFactory factory = new ReplaySafeLoggerFactory(context);

        Logger logger = factory.getLogger("com.example.Test");
        assertEquals("com.example.Test", logger.getName());
    }

    @Test
    void getLogger_producesLoggerThatSuppressesDuringReplay() {
        ILoggerFactory underlying = mock(ILoggerFactory.class);
        when(underlying.getLogger("test")).thenReturn(mockLogger);

        // Context is replaying
        TaskOrchestrationContext context = createMinimalContext(true, underlying);
        ReplaySafeLoggerFactory factory = new ReplaySafeLoggerFactory(context);

        Logger logger = factory.getLogger("test");
        logger.info("should be suppressed");
        verify(mockLogger, never()).info(anyString());
    }

    @Test
    void getLogger_producesLoggerThatForwardsWhenNotReplaying() {
        ILoggerFactory underlying = mock(ILoggerFactory.class);
        when(underlying.getLogger("test")).thenReturn(mockLogger);

        // Context is NOT replaying
        TaskOrchestrationContext context = createMinimalContext(false, underlying);
        ReplaySafeLoggerFactory factory = new ReplaySafeLoggerFactory(context);

        Logger logger = factory.getLogger("test");
        logger.info("should be forwarded");
        verify(mockLogger).info("should be forwarded");
    }

    // -----------------------------------------------------------------------
    // Caching in OrchestrationContextImpl — verify same instance returned
    // -----------------------------------------------------------------------

    @Test
    void getReplaySafeLoggerFactory_defaultAllocatesNewInstance() {
        ILoggerFactory underlying = mock(ILoggerFactory.class);
        TaskOrchestrationContext context = createMinimalContext(false, underlying);

        // Default method (no caching) — returns different instances
        ILoggerFactory f1 = context.getReplaySafeLoggerFactory();
        ILoggerFactory f2 = context.getReplaySafeLoggerFactory();
        assertNotSame(f1, f2);
    }

    // -----------------------------------------------------------------------
    // Wrapper-context delegation — no double-wrapping
    // Mirrors .NET's ChecksReplayOnce test (F-01/F-10 fix)
    // Uses mismatched replay flags to prove the wrapper's gate is consulted
    // -----------------------------------------------------------------------

    @Test
    void wrapperContext_doesNotDoubleWrapLoggers() {
        ILoggerFactory underlying = mock(ILoggerFactory.class);
        when(underlying.getLogger("test")).thenReturn(mockLogger);

        // Inner context is NOT replaying
        TaskOrchestrationContext innerContext = createMinimalContext(false, underlying);

        // Wrapper context IS replaying — and delegates getLoggerFactory() to inner's replay-safe factory
        // If double-wrapping occurred, the inner (non-replaying) gate would also be checked,
        // producing inconsistent behavior
        TaskOrchestrationContext wrapperContext = new MinimalContext(true, null) {
            @Override
            public ILoggerFactory getLoggerFactory() {
                return innerContext.getReplaySafeLoggerFactory();
            }
        };

        // createReplaySafeLogger on the wrapper should unwrap and produce a logger
        // that checks isReplaying on the WRAPPER context (which is replaying)
        Logger logger = wrapperContext.createReplaySafeLogger("test");
        assertNotNull(logger);
        assertTrue(logger instanceof ReplaySafeLogger);

        // The wrapper is replaying, so the log should be suppressed
        logger.info("should be suppressed");
        verify(mockLogger, never()).info(anyString());
    }

    // -----------------------------------------------------------------------
    // Cycle detection — mirrors .NET's cyclic-factory test
    // -----------------------------------------------------------------------

    @Test
    void cyclicFactory_throwsIllegalStateException() {
        // A context whose getLoggerFactory() returns this.getReplaySafeLoggerFactory()
        // creates an infinite loop
        TaskOrchestrationContext cyclicContext = new MinimalContext(false, null) {
            @Override
            public ILoggerFactory getLoggerFactory() {
                return this.getReplaySafeLoggerFactory();
            }
        };

        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> cyclicContext.createReplaySafeLogger("test"));
        assertTrue(ex.getMessage().contains("Maximum unwrap depth exceeded"));
    }

    // -----------------------------------------------------------------------
    // F-02: Verify wrapper-context forwards when inner is not replaying
    // (complementary to the suppression test above — proves single gate)
    // -----------------------------------------------------------------------

    @Test
    void wrapperContext_forwardsWhenWrapperNotReplaying() {
        ILoggerFactory underlying = mock(ILoggerFactory.class);
        when(underlying.getLogger("test")).thenReturn(mockLogger);

        // Inner context IS replaying (but shouldn't matter — wrapper's gate is what's consulted)
        TaskOrchestrationContext innerContext = createMinimalContext(true, underlying);

        // Wrapper is NOT replaying
        TaskOrchestrationContext wrapperContext = new MinimalContext(false, null) {
            @Override
            public ILoggerFactory getLoggerFactory() {
                return innerContext.getReplaySafeLoggerFactory();
            }
        };

        Logger logger = wrapperContext.createReplaySafeLogger("test");
        logger.info("should be forwarded");
        verify(mockLogger).info("should be forwarded");
    }

    // -----------------------------------------------------------------------
    // F-05: Fluent API end-to-end — verifies atInfo().log() is gated
    // -----------------------------------------------------------------------

    @Test
    void fluentApi_suppressedDuringReplay() {
        ILoggerFactory underlying = mock(ILoggerFactory.class);
        when(underlying.getLogger("test")).thenReturn(mockLogger);

        TaskOrchestrationContext context = createMinimalContext(true, underlying);
        Logger logger = context.createReplaySafeLogger("test");

        // atInfo() calls makeLoggingEventBuilder() which should return NOP during replay
        // The NOP builder's log() is a no-op, so mockLogger should never see the call
        logger.atInfo().log("fluent msg");
        verify(mockLogger, never()).info(anyString());
    }

    @Test
    void fluentApi_forwardedWhenNotReplaying() {
        ILoggerFactory underlying = mock(ILoggerFactory.class);
        when(underlying.getLogger("test")).thenReturn(mockLogger);
        when(mockLogger.isInfoEnabled()).thenReturn(true);
        // makeLoggingEventBuilder on the real mock needs to return a real builder
        // that will call back into mockLogger. We use a mock builder to verify log() is called.
        org.slf4j.spi.LoggingEventBuilder realBuilder = mock(org.slf4j.spi.LoggingEventBuilder.class);
        when(mockLogger.makeLoggingEventBuilder(org.slf4j.event.Level.INFO)).thenReturn(realBuilder);

        TaskOrchestrationContext context = createMinimalContext(false, underlying);
        Logger logger = context.createReplaySafeLogger("test");

        logger.atInfo().log("fluent msg");
        // The builder's log() should have been called (not NOP)
        verify(realBuilder).log("fluent msg");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private TaskOrchestrationContext createMinimalContext(boolean isReplaying, ILoggerFactory loggerFactory) {
        return new MinimalContext(isReplaying, loggerFactory);
    }

    /**
     * Minimal concrete implementation of TaskOrchestrationContext for tests.
     * Only getIsReplaying() and getLoggerFactory() are meaningful; everything else stubs.
     */
    private static class MinimalContext implements TaskOrchestrationContext {
        private final boolean isReplaying;
        private final ILoggerFactory loggerFactory;

        MinimalContext(boolean isReplaying, ILoggerFactory loggerFactory) {
            this.isReplaying = isReplaying;
            this.loggerFactory = loggerFactory;
        }

        @Override public boolean getIsReplaying() { return isReplaying; }

        @Override
        public ILoggerFactory getLoggerFactory() {
            return loggerFactory != null ? loggerFactory : TaskOrchestrationContext.super.getLoggerFactory();
        }

        // Stubs for all other interface methods
        @Override public String getName() { return "test"; }
        @Override public <V> V getInput(Class<V> t) { return null; }
        @Override public String getInstanceId() { return "id"; }
        @Override public Instant getCurrentInstant() { return Instant.now(); }
        @Override public String getVersion() { return ""; }
        @Override public <V> Task<List<V>> allOf(List<Task<V>> tasks) { return null; }
        @Override public Task<Task<?>> anyOf(List<Task<?>> tasks) { return null; }
        @Override public <V> Task<V> callActivity(String name, Object input, TaskOptions options, Class<V> returnType) { return null; }
        @Override public <V> Task<V> callSubOrchestrator(String name, Object input, String instanceId, TaskOptions options, Class<V> returnType) { return null; }
        @Override public Task<Void> createTimer(Duration delay) { return null; }
        @Override public Task<Void> createTimer(ZonedDateTime zonedDateTime) { return null; }
        @Override public <V> Task<V> waitForExternalEvent(String name, Duration timeout, Class<V> dataType) { return null; }
        @Override public void complete(Object output) {}
        @Override public void continueAsNew(Object input, boolean preserveUnprocessedEvents) {}
        @Override public UUID newUUID() { return UUID.randomUUID(); }
        @Override public void setCustomStatus(Object customStatus) {}
        @Override public void clearCustomStatus() {}
        @Override public void sendEvent(String instanceId, String eventName, Object eventData) {}
        @Override public void signalEntity(EntityInstanceId entityId, String operationName, Object input, SignalEntityOptions options) {}
        @Override public <V> Task<V> callEntity(EntityInstanceId entityId, String operationName, Object input, Class<V> returnType) { return null; }
        @Override public <V> Task<V> callEntity(EntityInstanceId entityId, String operationName, Object input, Class<V> returnType, CallEntityOptions options) { return null; }
        @Override public Task<AutoCloseable> lockEntities(List<EntityInstanceId> entityIds) { return null; }
        @Override public boolean isInCriticalSection() { return false; }
        @Override public List<EntityInstanceId> getLockedEntities() { return Collections.emptyList(); }
    }
}
