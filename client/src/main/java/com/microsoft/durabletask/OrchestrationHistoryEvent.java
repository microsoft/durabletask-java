// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Transport-neutral representation of a single orchestration history event.
 * <p>
 * This type decouples the public SDK API from the underlying gRPC/protobuf transport types,
 * ensuring API stability even if the wire format evolves. It is returned by
 * {@link DurableTaskClient#getOrchestrationHistory(String)}.
 */
public final class OrchestrationHistoryEvent {

    private final int eventId;
    private final Instant timestamp;
    private final String eventType;
    private final Map<String, Object> data;

    /**
     * Creates a new {@code OrchestrationHistoryEvent}.
     *
     * @param eventId   the sequence ID of the event within the orchestration history
     * @param timestamp the UTC timestamp when the event was recorded
     * @param eventType the type name of the event (e.g., "ExecutionStarted", "TaskScheduled")
     * @param data      the event-specific data fields, or an empty map if none
     */
    public OrchestrationHistoryEvent(
            int eventId,
            @Nonnull Instant timestamp,
            @Nonnull String eventType,
            @Nonnull Map<String, Object> data) {
        if (timestamp == null) {
            throw new IllegalArgumentException("timestamp must not be null.");
        }
        if (eventType == null || eventType.isEmpty()) {
            throw new IllegalArgumentException("eventType must not be null or empty.");
        }
        if (data == null) {
            throw new IllegalArgumentException("data must not be null.");
        }
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.eventType = eventType;
        this.data = Collections.unmodifiableMap(new HashMap<>(data));
    }

    /**
     * Gets the sequence ID of this event within the orchestration history.
     *
     * @return the event ID
     */
    public int getEventId() {
        return this.eventId;
    }

    /**
     * Gets the UTC timestamp when this event was recorded.
     *
     * @return the event timestamp
     */
    @Nonnull
    public Instant getTimestamp() {
        return this.timestamp;
    }

    /**
     * Gets the type name of this event (e.g., "ExecutionStarted", "TaskScheduled", "TaskCompleted").
     *
     * @return the event type name
     */
    @Nonnull
    public String getEventType() {
        return this.eventType;
    }

    /**
     * Gets the event-specific data fields as an unmodifiable map.
     * <p>
     * The keys and structure of this map depend on the event type. For example, an
     * "ExecutionStarted" event may contain "name", "input", and "version" fields.
     *
     * @return unmodifiable map of event data
     */
    @Nonnull
    public Map<String, Object> getData() {
        return this.data;
    }
}
