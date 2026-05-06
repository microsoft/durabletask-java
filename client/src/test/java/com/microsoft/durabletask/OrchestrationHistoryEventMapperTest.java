// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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

    @SuppressWarnings("unchecked")
    @Test
    void fromProto_taskScheduledWithTags_mapsTagsAsMap() {
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(10)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067200).build())
                .setTaskScheduled(TaskScheduledEvent.newBuilder()
                        .setName("MyActivity")
                        .putTags("env", "production")
                        .putTags("region", "westus2")
                        .build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertEquals("TaskScheduled", event.getEventType());
        Object tags = event.getData().get("tags");
        assertNotNull(tags, "tags should be present in data");
        assertInstanceOf(Map.class, tags, "tags should be a Map");
        Map<String, Object> tagsMap = (Map<String, Object>) tags;
        assertEquals("production", tagsMap.get("env"));
        assertEquals("westus2", tagsMap.get("region"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void fromProto_entityLockRequested_mapsLockSetAsList() {
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(11)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067200).build())
                .setEntityLockRequested(EntityLockRequestedEvent.newBuilder()
                        .setCriticalSectionId("cs-1")
                        .addLockSet("entity1")
                        .addLockSet("entity2")
                        .addLockSet("entity3")
                        .build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertEquals("EntityLockRequested", event.getEventType());
        Object lockSet = event.getData().get("lockSet");
        assertNotNull(lockSet, "lockSet should be present in data");
        assertInstanceOf(List.class, lockSet, "lockSet should be a List");
        List<Object> lockList = (List<Object>) lockSet;
        assertEquals(3, lockList.size());
        assertEquals("entity1", lockList.get(0));
        assertEquals("entity2", lockList.get(1));
        assertEquals("entity3", lockList.get(2));
    }

    @SuppressWarnings("unchecked")
    @Test
    void fromProto_executionStartedWithTags_mapsTagsAsMap() {
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(12)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067200).build())
                .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                        .setName("MyOrch")
                        .putTags("owner", "team-a")
                        .build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertEquals("ExecutionStarted", event.getEventType());
        Object tags = event.getData().get("tags");
        assertNotNull(tags, "tags should be present");
        assertInstanceOf(Map.class, tags);
        Map<String, Object> tagsMap = (Map<String, Object>) tags;
        assertEquals("team-a", tagsMap.get("owner"));
    }

    @Test
    void fromProto_emptyRepeatedField_notIncludedInData() {
        // Proto repeated fields with no elements are not included in getAllFields()
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(13)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067200).build())
                .setEntityLockRequested(EntityLockRequestedEvent.newBuilder()
                        .setCriticalSectionId("cs-empty")
                        .build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        // lockSet should not be present since it's empty (proto omits default-valued fields)
        assertFalse(event.getData().containsKey("lockSet"),
                "Empty repeated field should not appear in data");
    }

    @SuppressWarnings("unchecked")
    @Test
    void fromProto_emptyMapField_notIncludedInData() {
        // Proto map fields with no entries are not included in getAllFields()
        HistoryEvent proto = HistoryEvent.newBuilder()
                .setEventId(14)
                .setTimestamp(Timestamp.newBuilder().setSeconds(1704067200).build())
                .setTaskScheduled(TaskScheduledEvent.newBuilder()
                        .setName("NoTagsActivity")
                        .build())
                .build();

        OrchestrationHistoryEvent event = OrchestrationHistoryEventMapper.fromProto(proto);

        assertFalse(event.getData().containsKey("tags"),
                "Empty map field should not appear in data");
    }
}
