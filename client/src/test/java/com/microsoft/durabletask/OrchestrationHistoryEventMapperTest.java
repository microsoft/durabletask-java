// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link OrchestrationHistoryEventMapper}.
 */
class OrchestrationHistoryEventMapperTest {

    @Test
    void fromProto_executionStarted_mapsCorrectly() {
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(0)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067200).build())
                .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                        .setName("MyOrchestration")
                        .setInput(StringValue.of("{\"key\":\"value\"}"))
                        .build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertEquals(0, event.getEventId());
        assertEquals("ExecutionStarted", event.getEventType());
        assertNotNull(event.getTimestamp());
        assertNotNull(event.getData());
        assertEquals("MyOrchestration", event.getData().get("name"));
    }

    @Test
    void fromProto_taskScheduled_mapsCorrectly() {
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(1)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067201).build())
                .setTaskScheduled(TaskScheduledEvent.newBuilder()
                        .setName("MyActivity")
                        .setInput(StringValue.of("hello"))
                        .build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertEquals(1, event.getEventId());
        assertEquals("TaskScheduled", event.getEventType());
        assertEquals("MyActivity", event.getData().get("name"));
    }

    @Test
    void fromProto_taskCompleted_mapsCorrectly() {
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(2)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067202).build())
                .setTaskCompleted(TaskCompletedEvent.newBuilder()
                        .setTaskScheduledId(1)
                        .setResult(StringValue.of("result-value"))
                        .build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertEquals(2, event.getEventId());
        assertEquals("TaskCompleted", event.getEventType());
        assertEquals(1, event.getData().get("taskScheduledId"));
    }

    @Test
    void fromProto_executionCompleted_mapsCorrectly() {
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(3)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067203).build())
                .setExecutionCompleted(ExecutionCompletedEvent.newBuilder()
                        .setOrchestrationStatus(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED)
                        .setResult(StringValue.of("final-output"))
                        .build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertEquals(3, event.getEventId());
        assertEquals("ExecutionCompleted", event.getEventType());
    }

    @Test
    void fromProto_timerCreated_mapsCorrectly() {
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(4)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067204).build())
                .setTimerCreated(TimerCreatedEvent.newBuilder()
                        .setFireAt(Timestamp.newBuilder().setSeconds(1704070800).build())
                        .build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertEquals(4, event.getEventId());
        assertEquals("TimerCreated", event.getEventType());
        assertNotNull(event.getData().get("fireAt"));
    }

    @Test
    void fromProto_noEventType_returnsUnknown() {
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(99)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067200).build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertEquals("Unknown", event.getEventType());
        assertTrue(event.getData().isEmpty());
    }

    @Test
    void fromProto_nullInput_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                OrchestrationHistoryEventMapper.fromProto(null));
    }

    @Test
    void fromProto_preservesEventId() {
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(42)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067200).build())
                .setOrchestratorStarted(OrchestratorStartedEvent.newBuilder().build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);
        assertEquals(42, event.getEventId());
    }

    @Test
    void fromProto_timestampConversion_isCorrect() {
        // 2024-01-01T00:00:00Z = 1704067200 seconds
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(0)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(1704067200)
                        .setNanos(500000000) // 0.5 seconds
                        .build())
                .setOrchestratorStarted(OrchestratorStartedEvent.newBuilder().build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertEquals(1704067200, event.getTimestamp().getEpochSecond());
        assertEquals(500000000, event.getTimestamp().getNano());
    }
}
