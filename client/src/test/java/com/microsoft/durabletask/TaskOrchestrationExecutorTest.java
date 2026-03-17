// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
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
}
