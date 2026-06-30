// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.history.ExecutionCompletedEvent;
import com.microsoft.durabletask.history.ExecutionStartedEvent;
import com.microsoft.durabletask.history.GenericEvent;
import com.microsoft.durabletask.history.HistoryEvent;
import com.microsoft.durabletask.history.HistoryStateEvent;
import com.microsoft.durabletask.history.OrchestrationState;
import com.microsoft.durabletask.history.SubOrchestrationInstanceCompletedEvent;
import com.microsoft.durabletask.history.TaskCompletedEvent;
import com.microsoft.durabletask.history.TaskFailedEvent;
import com.microsoft.durabletask.history.TimerFiredEvent;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void throwsWhenEventTypeNotSet() {
        OrchestratorService.HistoryEvent proto = baseEvent(10).build();

        assertThrows(IllegalArgumentException.class, () -> HistoryEventConverter.fromProto(proto));
    }
}
