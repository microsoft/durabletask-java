// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link OrchestrationHistoryEvent}.
 */
class OrchestrationHistoryEventTest {

    @Test
    void constructor_validValues() {
        Map<String, Object> data = new HashMap<>();
        data.put("name", "MyOrch");
        Instant ts = Instant.parse("2026-01-01T00:00:00Z");

        OrchestrationHistoryEvent event = new OrchestrationHistoryEvent(0, ts, "ExecutionStarted", data);

        assertEquals(0, event.getEventId());
        assertEquals(ts, event.getTimestamp());
        assertEquals("ExecutionStarted", event.getEventType());
        assertEquals("MyOrch", event.getData().get("name"));
    }

    @Test
    void constructor_nullTimestamp_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new OrchestrationHistoryEvent(0, null, "ExecutionStarted", Collections.emptyMap()));
    }

    @Test
    void constructor_nullEventType_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new OrchestrationHistoryEvent(0, Instant.now(), null, Collections.emptyMap()));
    }

    @Test
    void constructor_emptyEventType_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new OrchestrationHistoryEvent(0, Instant.now(), "", Collections.emptyMap()));
    }

    @Test
    void constructor_nullData_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new OrchestrationHistoryEvent(0, Instant.now(), "ExecutionStarted", null));
    }

    @Test
    void data_isUnmodifiable() {
        Map<String, Object> data = new HashMap<>();
        data.put("key", "value");

        OrchestrationHistoryEvent event = new OrchestrationHistoryEvent(
                0, Instant.now(), "ExecutionStarted", data);

        assertThrows(UnsupportedOperationException.class, () ->
                event.getData().put("another", "value"));
    }
}
