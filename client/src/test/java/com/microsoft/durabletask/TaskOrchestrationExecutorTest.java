// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TaskOrchestrationExecutor.
 */
public class TaskOrchestrationExecutorTest {

    private static final Logger logger = Logger.getLogger(TaskOrchestrationExecutorTest.class.getName());

    @Test
    void execute_unregisteredOrchestrationType_failsWithDescriptiveMessage() {
        // Arrange: create executor with no registered orchestrations
        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(
                factories,
                new JacksonDataConverter(),
                Duration.ofDays(3),
                logger,
                null);

        String unknownName = "NonExistentOrchestration";

        // Build history events simulating an orchestration start
        HistoryEvent orchestratorStarted = HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                .build();

        HistoryEvent executionStarted = HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                        .setName(unknownName)
                        .setVersion(StringValue.of(""))
                        .setInput(StringValue.of("\"test-input\""))
                        .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                .setInstanceId("test-instance-id")
                                .build())
                        .build())
                .build();

        HistoryEvent orchestratorCompleted = HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                .build();

        List<HistoryEvent> pastEvents = Arrays.asList(orchestratorStarted, executionStarted, orchestratorCompleted);
        List<HistoryEvent> newEvents = Collections.emptyList();

        // Act
        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        // Assert: the result should contain a CompleteOrchestrationAction with FAILED status
        // and a failure message mentioning the unknown orchestration name
        OrchestratorAction action = result.getActions().iterator().next();
        assertTrue(action.hasCompleteOrchestration(), "Expected a CompleteOrchestrationAction");

        CompleteOrchestrationAction completeAction = action.getCompleteOrchestration();
        assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_FAILED, completeAction.getOrchestrationStatus());
        assertTrue(completeAction.hasFailureDetails(), "Expected failure details");

        TaskFailureDetails failureDetails = completeAction.getFailureDetails();
        assertEquals("java.lang.IllegalStateException", failureDetails.getErrorType());
        assertTrue(failureDetails.getErrorMessage().contains(unknownName),
                "Error message should contain the orchestration name: " + failureDetails.getErrorMessage());
        assertTrue(failureDetails.getErrorMessage().contains("worker"),
                "Error message should mention workers: " + failureDetails.getErrorMessage());
    }

    @Test
    void execute_propagatesTraceContextToActivities() {
        // Arrange: create an orchestration that calls an activity
        String orchName = "TestOrchestration";
        String activityName = "TestActivity";
        TraceContext parentTrace = TraceContext.newBuilder()
                .setTraceParent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
                .setTraceState(StringValue.of("vendorname=opaqueValue"))
                .build();

        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put(orchName, new TaskOrchestrationFactory() {
            @Override
            public String getName() { return orchName; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    ctx.callActivity(activityName, null, String.class);
                };
            }
        });

        TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(
                factories, new JacksonDataConverter(), Duration.ofDays(3), logger, null);

        // Build history events with trace context
        List<HistoryEvent> newEvents = Arrays.asList(
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                                .setName(orchName)
                                .setVersion(StringValue.of(""))
                                .setInput(StringValue.of("\"test\""))
                                .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                        .setInstanceId("test-instance")
                                        .build())
                                .setParentTraceContext(parentTrace)
                                .build())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                        .build()
        );

        // Act
        TaskOrchestratorResult result = executor.execute(Collections.emptyList(), newEvents, null);

        // Assert: find the ScheduleTaskAction and verify it has parentTraceContext set
        List<OrchestratorAction> actions = new ArrayList<>(result.getActions());
        OrchestratorAction scheduleAction = actions.stream()
                .filter(OrchestratorAction::hasScheduleTask)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected a ScheduleTaskAction"));

        ScheduleTaskAction taskAction = scheduleAction.getScheduleTask();
        assertEquals(activityName, taskAction.getName());
        assertTrue(taskAction.hasParentTraceContext(), "ScheduleTaskAction should have parentTraceContext");
        // The propagated context should have the same trace ID but a new span ID (client span)
        String originalTraceId = parentTrace.getTraceParent().split("-")[1];
        String propagatedTraceId = taskAction.getParentTraceContext().getTraceParent().split("-")[1];
        assertEquals(originalTraceId, propagatedTraceId, "Should share the same trace ID");
        String originalSpanId = parentTrace.getTraceParent().split("-")[2];
        String propagatedSpanId = taskAction.getParentTraceContext().getTraceParent().split("-")[2];
        assertNotEquals(originalSpanId, propagatedSpanId, "Should have a new client span ID");
    }

    @Test
    void execute_propagatesTraceContextToSubOrchestrations() {
        // Arrange: create an orchestration that calls a sub-orchestration
        String orchName = "ParentOrch";
        String subOrchName = "ChildOrch";
        TraceContext parentTrace = TraceContext.newBuilder()
                .setTraceParent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
                .build();

        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put(orchName, new TaskOrchestrationFactory() {
            @Override
            public String getName() { return orchName; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    ctx.callSubOrchestrator(subOrchName, null, String.class);
                };
            }
        });

        TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(
                factories, new JacksonDataConverter(), Duration.ofDays(3), logger, null);

        List<HistoryEvent> newEvents = Arrays.asList(
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                                .setName(orchName)
                                .setVersion(StringValue.of(""))
                                .setInput(StringValue.of("\"test\""))
                                .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                        .setInstanceId("parent-instance")
                                        .build())
                                .setParentTraceContext(parentTrace)
                                .build())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                        .build()
        );

        // Act
        TaskOrchestratorResult result = executor.execute(Collections.emptyList(), newEvents, null);

        // Assert: find the CreateSubOrchestrationAction and verify it has parentTraceContext
        List<OrchestratorAction> actions = new ArrayList<>(result.getActions());
        OrchestratorAction subOrchAction = actions.stream()
                .filter(OrchestratorAction::hasCreateSubOrchestration)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected a CreateSubOrchestrationAction"));

        CreateSubOrchestrationAction createSubOrch = subOrchAction.getCreateSubOrchestration();
        assertEquals(subOrchName, createSubOrch.getName());
        assertTrue(createSubOrch.hasParentTraceContext(), "CreateSubOrchestrationAction should have parentTraceContext");
        // The propagated context should have the same trace ID but a new span ID (client span)
        String originalTraceId = parentTrace.getTraceParent().split("-")[1];
        String propagatedTraceId = createSubOrch.getParentTraceContext().getTraceParent().split("-")[1];
        assertEquals(originalTraceId, propagatedTraceId, "Should share the same trace ID");
    }

    @Test
    void execute_noTraceContext_actionsDoNotHaveTraceContext() {
        // Arrange: orchestration without trace context
        String orchName = "NoTraceOrch";

        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put(orchName, new TaskOrchestrationFactory() {
            @Override
            public String getName() { return orchName; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    ctx.callActivity("SomeActivity", null, String.class);
                };
            }
        });

        TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(
                factories, new JacksonDataConverter(), Duration.ofDays(3), logger, null);

        List<HistoryEvent> newEvents = Arrays.asList(
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                                .setName(orchName)
                                .setVersion(StringValue.of(""))
                                .setInput(StringValue.of("\"test\""))
                                .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                        .setInstanceId("no-trace-instance")
                                        .build())
                                .build())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                        .build()
        );

        // Act
        TaskOrchestratorResult result = executor.execute(Collections.emptyList(), newEvents, null);

        // Assert: actions should not have trace context when none was provided
        List<OrchestratorAction> actions = new ArrayList<>(result.getActions());
        OrchestratorAction scheduleAction = actions.stream()
                .filter(OrchestratorAction::hasScheduleTask)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected a ScheduleTaskAction"));

        assertFalse(scheduleAction.getScheduleTask().hasParentTraceContext(),
                "ScheduleTaskAction should not have parentTraceContext when none was provided");
    }

    // region Parent Instance Tests

    @Test
    void execute_withParentInstance_getParentReturnsParent() {
        // Arrange: orchestration that captures getParent()
        String orchName = "ChildOrch";
        ParentOrchestrationInstance[] captured = new ParentOrchestrationInstance[1];

        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put(orchName, new TaskOrchestrationFactory() {
            @Override
            public String getName() { return orchName; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    captured[0] = ctx.getParent();
                    ctx.complete("done");
                };
            }
        });

        TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(
                factories, new JacksonDataConverter(), Duration.ofDays(3), logger, null);

        List<HistoryEvent> newEvents = Arrays.asList(
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                                .setName(orchName)
                                .setVersion(StringValue.of(""))
                                .setInput(StringValue.of("\"test\""))
                                .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                        .setInstanceId("child-instance")
                                        .build())
                                .setParentInstance(ParentInstanceInfo.newBuilder()
                                        .setName(StringValue.of("ParentOrch"))
                                        .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                                .setInstanceId("parent-123")
                                                .build())
                                        .build())
                                .build())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                        .build()
        );

        // Act
        executor.execute(Collections.emptyList(), newEvents, null);

        // Assert
        assertNotNull(captured[0], "getParent() should not be null for a sub-orchestration");
        assertEquals("ParentOrch", captured[0].getName());
        assertEquals("parent-123", captured[0].getInstanceId());
    }

    @Test
    void execute_withoutParentInstance_getParentReturnsNull() {
        // Arrange: orchestration without parent instance
        String orchName = "StandaloneOrch";
        ParentOrchestrationInstance[] captured = new ParentOrchestrationInstance[1];
        boolean[] wasCalled = {false};

        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put(orchName, new TaskOrchestrationFactory() {
            @Override
            public String getName() { return orchName; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    captured[0] = ctx.getParent();
                    wasCalled[0] = true;
                    ctx.complete("done");
                };
            }
        });

        TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(
                factories, new JacksonDataConverter(), Duration.ofDays(3), logger, null);

        List<HistoryEvent> newEvents = Arrays.asList(
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                                .setName(orchName)
                                .setVersion(StringValue.of(""))
                                .setInput(StringValue.of("\"test\""))
                                .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                        .setInstanceId("standalone-instance")
                                        .build())
                                .build())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                        .build()
        );

        // Act
        executor.execute(Collections.emptyList(), newEvents, null);

        // Assert
        assertTrue(wasCalled[0], "Orchestrator should have been called");
        assertNull(captured[0], "getParent() should be null for a standalone orchestration");
    }

    @Test
    void execute_withParentInstance_preservesExactValues() {
        // Arrange: use mixed casing and special characters
        String orchName = "ChildOrch";
        String parentName = "Parent.Orch-V2";
        String parentInstanceId = "abc-DEF-123_special!@#";
        ParentOrchestrationInstance[] captured = new ParentOrchestrationInstance[1];

        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put(orchName, new TaskOrchestrationFactory() {
            @Override
            public String getName() { return orchName; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    captured[0] = ctx.getParent();
                    ctx.complete("done");
                };
            }
        });

        TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(
                factories, new JacksonDataConverter(), Duration.ofDays(3), logger, null);

        List<HistoryEvent> newEvents = Arrays.asList(
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                                .setName(orchName)
                                .setVersion(StringValue.of(""))
                                .setInput(StringValue.of("\"test\""))
                                .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                        .setInstanceId("child-instance")
                                        .build())
                                .setParentInstance(ParentInstanceInfo.newBuilder()
                                        .setName(StringValue.of(parentName))
                                        .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                                .setInstanceId(parentInstanceId)
                                                .build())
                                        .build())
                                .build())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                        .build()
        );

        // Act
        executor.execute(Collections.emptyList(), newEvents, null);

        // Assert: values must match exactly, no normalization
        assertNotNull(captured[0]);
        assertEquals(parentName, captured[0].getName());
        assertEquals(parentInstanceId, captured[0].getInstanceId());
    }

    @Test
    void execute_withParentInstance_emptyFields_acceptsValues() {
        // Arrange: parent instance with empty/default StringValue fields
        String orchName = "ChildOrch";
        ParentOrchestrationInstance[] captured = new ParentOrchestrationInstance[1];

        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put(orchName, new TaskOrchestrationFactory() {
            @Override
            public String getName() { return orchName; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    captured[0] = ctx.getParent();
                    ctx.complete("done");
                };
            }
        });

        TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(
                factories, new JacksonDataConverter(), Duration.ofDays(3), logger, null);

        List<HistoryEvent> newEvents = Arrays.asList(
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                                .setName(orchName)
                                .setVersion(StringValue.of(""))
                                .setInput(StringValue.of("\"test\""))
                                .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                        .setInstanceId("child-instance")
                                        .build())
                                .setParentInstance(ParentInstanceInfo.newBuilder()
                                        .setName(StringValue.of(""))
                                        .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                                .setInstanceId("")
                                                .build())
                                        .build())
                                .build())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                        .build()
        );

        // Act
        executor.execute(Collections.emptyList(), newEvents, null);

        // Assert: permissive — empty values accepted as-is, matching .NET behavior
        assertNotNull(captured[0], "getParent() should not be null when parentInstance is present");
        assertEquals("", captured[0].getName());
        assertEquals("", captured[0].getInstanceId());
    }

    @Test
    void taskOrchestrationContext_defaultGetParent_returnsNull() {
        // Minimal implementation that does NOT override getParent().
        // All abstract methods are stubbed to satisfy the interface contract.
        TaskOrchestrationContext minimalContext = new TaskOrchestrationContext() {
            @Override public String getName() { return "test"; }
            @Override public <V> V getInput(Class<V> t) { return null; }
            @Override public String getInstanceId() { return "id"; }
            @Override public Instant getCurrentInstant() { return Instant.now(); }
            @Override public boolean getIsReplaying() { return false; }
            @Override public String getVersion() { return ""; }
            @Override public <V> Task<List<V>> allOf(List<Task<V>> tasks) { return null; }
            @Override public Task<Task<?>> anyOf(List<Task<?>> tasks) { return null; }
            @Override public <V> Task<V> callActivity(String name, Object input, TaskOptions options, Class<V> returnType) { return null; }
            @Override public <V> Task<V> callSubOrchestrator(String name, Object input, String instanceId, TaskOptions options, Class<V> returnType) { return null; }
            @Override public Task<Void> createTimer(Duration delay) { return null; }
            @Override public Task<Void> createTimer(ZonedDateTime zonedDateTime) { return null; }
            @Override public <V> Task<V> waitForExternalEvent(String name, Duration timeout, Class<V> dataType) { return null; }
            @Override public void continueAsNew(Object input, boolean preserveUnprocessedEvents) {}
            @Override public void sendEvent(String instanceId, String eventName, Object eventData) {}
            @Override public void complete(Object output) {}
            @Override public UUID newUUID() { return UUID.randomUUID(); }
            @Override public void signalEntity(EntityInstanceId entityId, String operationName, Object input, SignalEntityOptions options) {}
            @Override public <V> Task<V> callEntity(EntityInstanceId entityId, String operationName, Object input, Class<V> returnType) { return null; }
            @Override public <V> Task<V> callEntity(EntityInstanceId entityId, String operationName, Object input, Class<V> returnType, CallEntityOptions options) { return null; }
            @Override public Task<AutoCloseable> lockEntities(List<EntityInstanceId> entityIds) { return null; }
            @Override public boolean isInCriticalSection() { return false; }
            @Override public List<EntityInstanceId> getLockedEntities() { return Collections.emptyList(); }
            @Override public void setCustomStatus(Object customStatus) {}
            @Override public void clearCustomStatus() {}
        };

        // Assert: default method returns null
        assertNull(minimalContext.getParent(), "Default getParent() should return null");
    }

    @Test
    void executorReplay_parentValueStableAcrossReplays() {
        // Arrange: simulate replay — ExecutionStartedEvent is in pastEvents,
        // new dispatch has OrchestratorStarted + OrchestratorCompleted.
        String orchName = "ChildOrch";
        ParentOrchestrationInstance[] captured = new ParentOrchestrationInstance[1];

        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put(orchName, new TaskOrchestrationFactory() {
            @Override
            public String getName() { return orchName; }
            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    captured[0] = ctx.getParent();
                    ctx.complete("done");
                };
            }
        });

        TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(
                factories, new JacksonDataConverter(), Duration.ofDays(3), logger, null);

        // Past events: the initial execution (already processed)
        List<HistoryEvent> pastEvents = Arrays.asList(
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                                .setName(orchName)
                                .setVersion(StringValue.of(""))
                                .setInput(StringValue.of("\"test\""))
                                .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                        .setInstanceId("child-instance")
                                        .build())
                                .setParentInstance(ParentInstanceInfo.newBuilder()
                                        .setName(StringValue.of("ParentOrch"))
                                        .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                                .setInstanceId("parent-456")
                                                .build())
                                        .build())
                                .build())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                        .build()
        );

        // New events: a new dispatch re-enters the orchestrator
        List<HistoryEvent> newEvents = Arrays.asList(
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                        .build(),
                HistoryEvent.newBuilder()
                        .setEventId(-1)
                        .setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                        .build()
        );

        // Act: execute with replay history
        executor.execute(pastEvents, newEvents, null);

        // Assert: parent is still available and unchanged during replay
        assertNotNull(captured[0], "getParent() should not be null during replay");
        assertEquals("ParentOrch", captured[0].getName());
        assertEquals("parent-456", captured[0].getInstanceId());
    }

    @Test
    void parentOrchestrationInstance_equalsAndHashCode() {
        ParentOrchestrationInstance a = new ParentOrchestrationInstance("Orch", "id-1");
        ParentOrchestrationInstance b = new ParentOrchestrationInstance("Orch", "id-1");
        ParentOrchestrationInstance c = new ParentOrchestrationInstance("Other", "id-1");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, null);
        assertEquals("ParentOrchestrationInstance{name='Orch', instanceId='id-1'}", a.toString());
    }

    // endregion
}
