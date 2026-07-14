// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.history.ContinueAsNewEvent;
import com.microsoft.durabletask.history.EntityLockGrantedEvent;
import com.microsoft.durabletask.history.EntityLockRequestedEvent;
import com.microsoft.durabletask.history.EntityOperationCalledEvent;
import com.microsoft.durabletask.history.EntityOperationCompletedEvent;
import com.microsoft.durabletask.history.EntityOperationFailedEvent;
import com.microsoft.durabletask.history.EntityOperationSignaledEvent;
import com.microsoft.durabletask.history.EntityUnlockSentEvent;
import com.microsoft.durabletask.history.EventRaisedEvent;
import com.microsoft.durabletask.history.EventSentEvent;
import com.microsoft.durabletask.history.ExecutionCompletedEvent;
import com.microsoft.durabletask.history.ExecutionResumedEvent;
import com.microsoft.durabletask.history.ExecutionRewoundEvent;
import com.microsoft.durabletask.history.ExecutionStartedEvent;
import com.microsoft.durabletask.history.ExecutionSuspendedEvent;
import com.microsoft.durabletask.history.ExecutionTerminatedEvent;
import com.microsoft.durabletask.history.GenericEvent;
import com.microsoft.durabletask.history.HistoryEvent;
import com.microsoft.durabletask.history.HistoryStateEvent;
import com.microsoft.durabletask.history.OrchestratorCompletedEvent;
import com.microsoft.durabletask.history.OrchestratorStartedEvent;
import com.microsoft.durabletask.history.OrchestrationState;
import com.microsoft.durabletask.history.SubOrchestrationInstanceCreatedEvent;
import com.microsoft.durabletask.history.SubOrchestrationInstanceCompletedEvent;
import com.microsoft.durabletask.history.SubOrchestrationInstanceFailedEvent;
import com.microsoft.durabletask.history.TaskCompletedEvent;
import com.microsoft.durabletask.history.TaskFailedEvent;
import com.microsoft.durabletask.history.TaskScheduledEvent;
import com.microsoft.durabletask.history.TimerCreatedEvent;
import com.microsoft.durabletask.history.TimerFiredEvent;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HistoryEventConverter}, which maps protobuf history events to the public
 * {@code com.microsoft.durabletask.history} domain model.
 */
public class HistoryEventConverterTest {

    private static final long EPOCH_SECONDS = 1_700_000_000L;
    private static final Instant EXPECTED_TIMESTAMP = Instant.ofEpochSecond(EPOCH_SECONDS);

    private static OrchestratorService.HistoryEvent.Builder baseEvent(int eventId) {
        return OrchestratorService.HistoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(Timestamp.newBuilder().setSeconds(EPOCH_SECONDS).build());
    }

    @Test
    void convertsExecutionStarted() {
        OrchestratorService.HistoryEvent proto = baseEvent(1)
                .setExecutionStarted(OrchestratorService.ExecutionStartedEvent.newBuilder()
                        .setName("MyOrchestration")
                        .setVersion(StringValue.of("2.0"))
                        .setInput(StringValue.of("\"hello\""))
                        .setOrchestrationInstance(OrchestratorService.OrchestrationInstance.newBuilder()
                                .setInstanceId("inst-1")
                                .setExecutionId(StringValue.of("exec-1"))))
                .build();

        HistoryEvent event = HistoryEventConverter.fromProto(proto);

        ExecutionStartedEvent started = assertInstanceOf(ExecutionStartedEvent.class, event);
        assertEquals(1, started.getEventId());
        assertEquals(EXPECTED_TIMESTAMP, started.getTimestamp());
        assertEquals("MyOrchestration", started.getName());
        assertEquals("2.0", started.getVersion());
        assertEquals("\"hello\"", started.getInput());
        assertNotNull(started.getOrchestrationInstance());
        assertEquals("inst-1", started.getOrchestrationInstance().getInstanceId());
        assertEquals("exec-1", started.getOrchestrationInstance().getExecutionId());
    }

    @Test
    void convertsExecutionCompletedWithResult() {
        OrchestratorService.HistoryEvent proto = baseEvent(2)
                .setExecutionCompleted(OrchestratorService.ExecutionCompletedEvent.newBuilder()
                        .setOrchestrationStatus(OrchestratorService.OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED)
                        .setResult(StringValue.of("\"done\"")))
                .build();

        HistoryEvent event = HistoryEventConverter.fromProto(proto);

        ExecutionCompletedEvent completed = assertInstanceOf(ExecutionCompletedEvent.class, event);
        assertEquals(OrchestrationRuntimeStatus.COMPLETED, completed.getOrchestrationStatus());
        assertEquals("\"done\"", completed.getResult());
        assertNull(completed.getFailureDetails());
    }

    @Test
    void convertsExecutionCompletedWithFailureDetails() {
        OrchestratorService.HistoryEvent proto = baseEvent(3)
                .setExecutionCompleted(OrchestratorService.ExecutionCompletedEvent.newBuilder()
                        .setOrchestrationStatus(OrchestratorService.OrchestrationStatus.ORCHESTRATION_STATUS_FAILED)
                        .setFailureDetails(OrchestratorService.TaskFailureDetails.newBuilder()
                                .setErrorType("java.lang.RuntimeException")
                                .setErrorMessage("boom")))
                .build();

        HistoryEvent event = HistoryEventConverter.fromProto(proto);

        ExecutionCompletedEvent completed = assertInstanceOf(ExecutionCompletedEvent.class, event);
        assertEquals(OrchestrationRuntimeStatus.FAILED, completed.getOrchestrationStatus());
        assertNotNull(completed.getFailureDetails());
        assertEquals("java.lang.RuntimeException", completed.getFailureDetails().getErrorType());
        assertEquals("boom", completed.getFailureDetails().getErrorMessage());
    }

    @Test
    void convertsTaskCompleted() {
        OrchestratorService.HistoryEvent proto = baseEvent(4)
                .setTaskCompleted(OrchestratorService.TaskCompletedEvent.newBuilder()
                        .setTaskScheduledId(7)
                        .setResult(StringValue.of("42")))
                .build();

        HistoryEvent event = HistoryEventConverter.fromProto(proto);

        TaskCompletedEvent completed = assertInstanceOf(TaskCompletedEvent.class, event);
        assertEquals(7, completed.getTaskScheduledId());
        assertEquals("42", completed.getResult());
    }

    @Test
    void convertsTaskFailedWithFailureDetails() {
        OrchestratorService.HistoryEvent proto = baseEvent(5)
                .setTaskFailed(OrchestratorService.TaskFailedEvent.newBuilder()
                        .setTaskScheduledId(9)
                        .setFailureDetails(OrchestratorService.TaskFailureDetails.newBuilder()
                                .setErrorType("java.io.IOException")
                                .setErrorMessage("disk full")))
                .build();

        HistoryEvent event = HistoryEventConverter.fromProto(proto);

        TaskFailedEvent failed = assertInstanceOf(TaskFailedEvent.class, event);
        assertEquals(9, failed.getTaskScheduledId());
        assertNotNull(failed.getFailureDetails());
        assertEquals("java.io.IOException", failed.getFailureDetails().getErrorType());
    }

    @Test
    void convertsSubOrchestrationCompleted() {
        OrchestratorService.HistoryEvent proto = baseEvent(6)
                .setSubOrchestrationInstanceCompleted(
                        OrchestratorService.SubOrchestrationInstanceCompletedEvent.newBuilder()
                                .setTaskScheduledId(3)
                                .setResult(StringValue.of("\"child-result\"")))
                .build();

        HistoryEvent event = HistoryEventConverter.fromProto(proto);

        SubOrchestrationInstanceCompletedEvent completed =
                assertInstanceOf(SubOrchestrationInstanceCompletedEvent.class, event);
        assertEquals(3, completed.getTaskScheduledId());
        assertEquals("\"child-result\"", completed.getResult());
    }

    @Test
    void convertsTimerFired() {
        Instant fireAt = Instant.ofEpochSecond(EPOCH_SECONDS + 60);
        OrchestratorService.HistoryEvent proto = baseEvent(7)
                .setTimerFired(OrchestratorService.TimerFiredEvent.newBuilder()
                        .setTimerId(11)
                        .setFireAt(Timestamp.newBuilder().setSeconds(fireAt.getEpochSecond()).build()))
                .build();

        HistoryEvent event = HistoryEventConverter.fromProto(proto);

        TimerFiredEvent fired = assertInstanceOf(TimerFiredEvent.class, event);
        assertEquals(11, fired.getTimerId());
        assertEquals(fireAt, fired.getFireAt());
    }

    @Test
    void convertsGenericEvent() {
        OrchestratorService.HistoryEvent proto = baseEvent(8)
                .setGenericEvent(OrchestratorService.GenericEvent.newBuilder()
                        .setData(StringValue.of("payload")))
                .build();

        HistoryEvent event = HistoryEventConverter.fromProto(proto);

        GenericEvent generic = assertInstanceOf(GenericEvent.class, event);
        assertEquals("payload", generic.getData());
    }

    @Test
    void convertsHistoryStateExposesFullOrchestrationState() {
        Instant created = Instant.ofEpochSecond(EPOCH_SECONDS - 100);
        Instant lastUpdated = Instant.ofEpochSecond(EPOCH_SECONDS - 50);
        OrchestratorService.HistoryEvent proto = baseEvent(9)
                .setHistoryState(OrchestratorService.HistoryStateEvent.newBuilder()
                        .setOrchestrationState(OrchestratorService.OrchestrationState.newBuilder()
                                .setInstanceId("inst-9")
                                .setName("MyOrchestration")
                                .setVersion(StringValue.of("1.5"))
                                .setOrchestrationStatus(
                                        OrchestratorService.OrchestrationStatus.ORCHESTRATION_STATUS_RUNNING)
                                .setCreatedTimestamp(Timestamp.newBuilder().setSeconds(created.getEpochSecond()))
                                .setLastUpdatedTimestamp(
                                        Timestamp.newBuilder().setSeconds(lastUpdated.getEpochSecond()))
                                .setInput(StringValue.of("\"in\""))
                                .setCustomStatus(StringValue.of("\"working\""))
                                .putTags("env", "prod")))
                .build();

        HistoryEvent event = HistoryEventConverter.fromProto(proto);

        HistoryStateEvent stateEvent = assertInstanceOf(HistoryStateEvent.class, event);
        OrchestrationState state = stateEvent.getState();
        assertNotNull(state);
        assertEquals("inst-9", state.getInstanceId());
        assertEquals("MyOrchestration", state.getName());
        assertEquals("1.5", state.getVersion());
        assertEquals(OrchestrationRuntimeStatus.RUNNING, state.getRuntimeStatus());
        assertEquals(created, state.getCreatedTime());
        assertEquals(lastUpdated, state.getLastUpdatedTime());
        assertEquals("\"in\"", state.getInput());
        assertEquals("\"working\"", state.getCustomStatus());
        assertNotNull(state.getTags());
        assertEquals("prod", state.getTags().get("env"));
    }

    @Test
    void convertsExecutionTerminated() {
        OrchestratorService.HistoryEvent proto = baseEvent(11)
                .setExecutionTerminated(OrchestratorService.ExecutionTerminatedEvent.newBuilder()
                        .setInput(StringValue.of("\"termination reason\""))
                        .setRecurse(true))
                .build();

        ExecutionTerminatedEvent terminated =
                assertInstanceOf(ExecutionTerminatedEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("\"termination reason\"", terminated.getInput());
        assertTrue(terminated.isRecurse());
    }

    @Test
    void convertsTaskScheduled() {
        OrchestratorService.HistoryEvent proto = baseEvent(12)
                .setTaskScheduled(OrchestratorService.TaskScheduledEvent.newBuilder()
                        .setName("SendEmail")
                        .setVersion(StringValue.of("v2"))
                        .setInput(StringValue.of("\"message\""))
                        .setParentTraceContext(OrchestratorService.TraceContext.newBuilder()
                                .setTraceParent("trace-parent")
                                .setTraceState(StringValue.of("trace-state")))
                        .putTags("operation", "email"))
                .build();

        TaskScheduledEvent scheduled =
                assertInstanceOf(TaskScheduledEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("SendEmail", scheduled.getName());
        assertEquals("v2", scheduled.getVersion());
        assertEquals("\"message\"", scheduled.getInput());
        assertNotNull(scheduled.getParentTraceContext());
        assertEquals("trace-parent", scheduled.getParentTraceContext().getTraceParent());
        assertEquals("trace-state", scheduled.getParentTraceContext().getTraceState());
        assertEquals("email", scheduled.getTags().get("operation"));
    }

    @Test
    void convertsSubOrchestrationInstanceCreated() {
        OrchestratorService.HistoryEvent proto = baseEvent(13)
                .setSubOrchestrationInstanceCreated(
                        OrchestratorService.SubOrchestrationInstanceCreatedEvent.newBuilder()
                                .setInstanceId("child-instance")
                                .setName("ChildOrchestrator")
                                .setVersion(StringValue.of("v3"))
                                .setInput(StringValue.of("\"child input\""))
                                .setParentTraceContext(OrchestratorService.TraceContext.newBuilder()
                                        .setTraceParent("child-trace-parent")
                                        .setTraceState(StringValue.of("child-trace-state")))
                                .putTags("source", "parent"))
                .build();

        SubOrchestrationInstanceCreatedEvent created = assertInstanceOf(
                SubOrchestrationInstanceCreatedEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("child-instance", created.getInstanceId());
        assertEquals("ChildOrchestrator", created.getName());
        assertEquals("v3", created.getVersion());
        assertEquals("\"child input\"", created.getInput());
        assertNotNull(created.getParentTraceContext());
        assertEquals("child-trace-parent", created.getParentTraceContext().getTraceParent());
        assertEquals("child-trace-state", created.getParentTraceContext().getTraceState());
        assertEquals("parent", created.getTags().get("source"));
    }

    @Test
    void convertsSubOrchestrationInstanceFailed() {
        OrchestratorService.HistoryEvent proto = baseEvent(14)
                .setSubOrchestrationInstanceFailed(
                        OrchestratorService.SubOrchestrationInstanceFailedEvent.newBuilder()
                                .setTaskScheduledId(23)
                                .setFailureDetails(OrchestratorService.TaskFailureDetails.newBuilder()
                                        .setErrorType("java.lang.IllegalStateException")
                                        .setErrorMessage("child failed")))
                .build();

        SubOrchestrationInstanceFailedEvent failed = assertInstanceOf(
                SubOrchestrationInstanceFailedEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals(23, failed.getTaskScheduledId());
        assertNotNull(failed.getFailureDetails());
        assertEquals("java.lang.IllegalStateException", failed.getFailureDetails().getErrorType());
        assertEquals("child failed", failed.getFailureDetails().getErrorMessage());
    }

    @Test
    void convertsTimerCreated() {
        Instant fireAt = Instant.ofEpochSecond(EPOCH_SECONDS + 120);
        OrchestratorService.HistoryEvent proto = baseEvent(15)
                .setTimerCreated(OrchestratorService.TimerCreatedEvent.newBuilder()
                        .setFireAt(Timestamp.newBuilder().setSeconds(fireAt.getEpochSecond())))
                .build();

        TimerCreatedEvent created = assertInstanceOf(TimerCreatedEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals(fireAt, created.getFireAt());
    }

    @Test
    void convertsOrchestratorStartedAndCompleted() {
        OrchestratorService.HistoryEvent startedProto = baseEvent(16)
                .setOrchestratorStarted(OrchestratorService.OrchestratorStartedEvent.newBuilder())
                .build();
        OrchestratorService.HistoryEvent completedProto = baseEvent(17)
                .setOrchestratorCompleted(OrchestratorService.OrchestratorCompletedEvent.newBuilder())
                .build();

        OrchestratorStartedEvent started =
                assertInstanceOf(OrchestratorStartedEvent.class, HistoryEventConverter.fromProto(startedProto));
        OrchestratorCompletedEvent completed =
                assertInstanceOf(OrchestratorCompletedEvent.class, HistoryEventConverter.fromProto(completedProto));

        assertEquals(16, started.getEventId());
        assertEquals(EXPECTED_TIMESTAMP, started.getTimestamp());
        assertEquals(17, completed.getEventId());
        assertEquals(EXPECTED_TIMESTAMP, completed.getTimestamp());
    }

    @Test
    void convertsEventSent() {
        OrchestratorService.HistoryEvent proto = baseEvent(18)
                .setEventSent(OrchestratorService.EventSentEvent.newBuilder()
                        .setInstanceId("target-instance")
                        .setName("ApprovalReceived")
                        .setInput(StringValue.of("true")))
                .build();

        EventSentEvent sent = assertInstanceOf(EventSentEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("target-instance", sent.getInstanceId());
        assertEquals("ApprovalReceived", sent.getName());
        assertEquals("true", sent.getInput());
    }

    @Test
    void convertsEventRaised() {
        OrchestratorService.HistoryEvent proto = baseEvent(19)
                .setEventRaised(OrchestratorService.EventRaisedEvent.newBuilder()
                        .setName("ApprovalReceived")
                        .setInput(StringValue.of("true")))
                .build();

        EventRaisedEvent raised = assertInstanceOf(EventRaisedEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("ApprovalReceived", raised.getName());
        assertEquals("true", raised.getInput());
    }

    @Test
    void convertsContinueAsNew() {
        OrchestratorService.HistoryEvent proto = baseEvent(20)
                .setContinueAsNew(OrchestratorService.ContinueAsNewEvent.newBuilder()
                        .setInput(StringValue.of("\"next generation\"")))
                .build();

        ContinueAsNewEvent continued =
                assertInstanceOf(ContinueAsNewEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("\"next generation\"", continued.getInput());
    }

    @Test
    void convertsExecutionSuspendedAndResumed() {
        OrchestratorService.HistoryEvent suspendedProto = baseEvent(21)
                .setExecutionSuspended(OrchestratorService.ExecutionSuspendedEvent.newBuilder()
                        .setInput(StringValue.of("\"maintenance\"")))
                .build();
        OrchestratorService.HistoryEvent resumedProto = baseEvent(22)
                .setExecutionResumed(OrchestratorService.ExecutionResumedEvent.newBuilder()
                        .setInput(StringValue.of("\"maintenance complete\"")))
                .build();

        ExecutionSuspendedEvent suspended =
                assertInstanceOf(ExecutionSuspendedEvent.class, HistoryEventConverter.fromProto(suspendedProto));
        ExecutionResumedEvent resumed =
                assertInstanceOf(ExecutionResumedEvent.class, HistoryEventConverter.fromProto(resumedProto));

        assertEquals("\"maintenance\"", suspended.getInput());
        assertEquals("\"maintenance complete\"", resumed.getInput());
    }

    @Test
    void convertsEntityOperationSignaled() {
        Instant scheduledTime = Instant.ofEpochSecond(EPOCH_SECONDS + 180);
        OrchestratorService.HistoryEvent proto = baseEvent(23)
                .setEntityOperationSignaled(OrchestratorService.EntityOperationSignaledEvent.newBuilder()
                        .setRequestId("signal-request")
                        .setOperation("increment")
                        .setScheduledTime(Timestamp.newBuilder().setSeconds(scheduledTime.getEpochSecond()))
                        .setInput(StringValue.of("5"))
                        .setTargetInstanceId(StringValue.of("@counter@one")))
                .build();

        EntityOperationSignaledEvent signaled =
                assertInstanceOf(EntityOperationSignaledEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("signal-request", signaled.getRequestId());
        assertEquals("increment", signaled.getOperation());
        assertEquals(scheduledTime, signaled.getScheduledTime());
        assertEquals("5", signaled.getInput());
        assertEquals("@counter@one", signaled.getTargetInstanceId());
    }

    @Test
    void convertsEntityOperationCalled() {
        Instant scheduledTime = Instant.ofEpochSecond(EPOCH_SECONDS + 240);
        OrchestratorService.HistoryEvent proto = baseEvent(24)
                .setEntityOperationCalled(OrchestratorService.EntityOperationCalledEvent.newBuilder()
                        .setRequestId("call-request")
                        .setOperation("get")
                        .setScheduledTime(Timestamp.newBuilder().setSeconds(scheduledTime.getEpochSecond()))
                        .setInput(StringValue.of("\"key\""))
                        .setParentInstanceId(StringValue.of("parent-instance"))
                        .setParentExecutionId(StringValue.of("parent-execution"))
                        .setTargetInstanceId(StringValue.of("@store@one")))
                .build();

        EntityOperationCalledEvent called =
                assertInstanceOf(EntityOperationCalledEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("call-request", called.getRequestId());
        assertEquals("get", called.getOperation());
        assertEquals(scheduledTime, called.getScheduledTime());
        assertEquals("\"key\"", called.getInput());
        assertEquals("parent-instance", called.getParentInstanceId());
        assertEquals("parent-execution", called.getParentExecutionId());
        assertEquals("@store@one", called.getTargetInstanceId());
    }

    @Test
    void convertsEntityOperationCompleted() {
        OrchestratorService.HistoryEvent proto = baseEvent(25)
                .setEntityOperationCompleted(OrchestratorService.EntityOperationCompletedEvent.newBuilder()
                        .setRequestId("completed-request")
                        .setOutput(StringValue.of("\"result\"")))
                .build();

        EntityOperationCompletedEvent completed =
                assertInstanceOf(EntityOperationCompletedEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("completed-request", completed.getRequestId());
        assertEquals("\"result\"", completed.getOutput());
    }

    @Test
    void convertsEntityOperationFailed() {
        OrchestratorService.HistoryEvent proto = baseEvent(26)
                .setEntityOperationFailed(OrchestratorService.EntityOperationFailedEvent.newBuilder()
                        .setRequestId("failed-request")
                        .setFailureDetails(OrchestratorService.TaskFailureDetails.newBuilder()
                                .setErrorType("java.lang.IllegalArgumentException")
                                .setErrorMessage("invalid operation")))
                .build();

        EntityOperationFailedEvent failed =
                assertInstanceOf(EntityOperationFailedEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("failed-request", failed.getRequestId());
        assertNotNull(failed.getFailureDetails());
        assertEquals("java.lang.IllegalArgumentException", failed.getFailureDetails().getErrorType());
        assertEquals("invalid operation", failed.getFailureDetails().getErrorMessage());
    }

    @Test
    void convertsEntityLockRequested() {
        OrchestratorService.HistoryEvent proto = baseEvent(27)
                .setEntityLockRequested(OrchestratorService.EntityLockRequestedEvent.newBuilder()
                        .setCriticalSectionId("critical-section")
                        .addAllLockSet(Arrays.asList("@account@one", "@account@two"))
                        .setPosition(1)
                        .setParentInstanceId(StringValue.of("parent-instance")))
                .build();

        EntityLockRequestedEvent requested =
                assertInstanceOf(EntityLockRequestedEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("critical-section", requested.getCriticalSectionId());
        assertEquals(Arrays.asList("@account@one", "@account@two"), requested.getLockSet());
        assertEquals(1, requested.getPosition());
        assertEquals("parent-instance", requested.getParentInstanceId());
    }

    @Test
    void convertsEntityLockGranted() {
        OrchestratorService.HistoryEvent proto = baseEvent(28)
                .setEntityLockGranted(OrchestratorService.EntityLockGrantedEvent.newBuilder()
                        .setCriticalSectionId("critical-section"))
                .build();

        EntityLockGrantedEvent granted =
                assertInstanceOf(EntityLockGrantedEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("critical-section", granted.getCriticalSectionId());
    }

    @Test
    void convertsEntityUnlockSent() {
        OrchestratorService.HistoryEvent proto = baseEvent(29)
                .setEntityUnlockSent(OrchestratorService.EntityUnlockSentEvent.newBuilder()
                        .setCriticalSectionId("critical-section")
                        .setParentInstanceId(StringValue.of("parent-instance"))
                        .setTargetInstanceId(StringValue.of("@account@one")))
                .build();

        EntityUnlockSentEvent unlockSent =
                assertInstanceOf(EntityUnlockSentEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("critical-section", unlockSent.getCriticalSectionId());
        assertEquals("parent-instance", unlockSent.getParentInstanceId());
        assertEquals("@account@one", unlockSent.getTargetInstanceId());
    }

    @Test
    void convertsExecutionRewound() {
        OrchestratorService.HistoryEvent proto = baseEvent(30)
                .setExecutionRewound(OrchestratorService.ExecutionRewoundEvent.newBuilder()
                        .setReason(StringValue.of("retry after fix"))
                        .setParentExecutionId(StringValue.of("parent-execution"))
                        .setInstanceId(StringValue.of("child-instance"))
                        .setParentTraceContext(OrchestratorService.TraceContext.newBuilder()
                                .setTraceParent("rewind-trace-parent")
                                .setTraceState(StringValue.of("rewind-trace-state")))
                        .setName(StringValue.of("ChildOrchestrator"))
                        .setVersion(StringValue.of("v4"))
                        .setInput(StringValue.of("\"rewind input\""))
                        .setParentInstance(OrchestratorService.ParentInstanceInfo.newBuilder()
                                .setTaskScheduledId(31)
                                .setName(StringValue.of("ParentOrchestrator"))
                                .setVersion(StringValue.of("v1"))
                                .setOrchestrationInstance(OrchestratorService.OrchestrationInstance.newBuilder()
                                        .setInstanceId("parent-instance")
                                        .setExecutionId(StringValue.of("parent-execution"))))
                        .putTags("reason", "repair"))
                .build();

        ExecutionRewoundEvent rewound =
                assertInstanceOf(ExecutionRewoundEvent.class, HistoryEventConverter.fromProto(proto));

        assertEquals("retry after fix", rewound.getReason());
        assertEquals("parent-execution", rewound.getParentExecutionId());
        assertEquals("child-instance", rewound.getInstanceId());
        assertNotNull(rewound.getParentTraceContext());
        assertEquals("rewind-trace-parent", rewound.getParentTraceContext().getTraceParent());
        assertEquals("rewind-trace-state", rewound.getParentTraceContext().getTraceState());
        assertEquals("ChildOrchestrator", rewound.getName());
        assertEquals("v4", rewound.getVersion());
        assertEquals("\"rewind input\"", rewound.getInput());
        assertNotNull(rewound.getParentInstance());
        assertEquals(31, rewound.getParentInstance().getTaskScheduledId());
        assertEquals("ParentOrchestrator", rewound.getParentInstance().getName());
        assertEquals("v1", rewound.getParentInstance().getVersion());
        assertNotNull(rewound.getParentInstance().getOrchestrationInstance());
        assertEquals("parent-instance", rewound.getParentInstance().getOrchestrationInstance().getInstanceId());
        assertEquals("parent-execution", rewound.getParentInstance().getOrchestrationInstance().getExecutionId());
        assertEquals("repair", rewound.getTags().get("reason"));
    }

    @Test
    void throwsWhenEventTypeNotSet() {
        OrchestratorService.HistoryEvent proto = baseEvent(10).build();

        assertThrows(IllegalArgumentException.class, () -> HistoryEventConverter.fromProto(proto));
    }
}
