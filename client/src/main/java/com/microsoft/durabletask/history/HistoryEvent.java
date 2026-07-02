// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import java.time.Instant;

/**
 * Base class for the events that make up an orchestration instance's execution history.
 * <p>
 * Instances are obtained from {@link com.microsoft.durabletask.DurableTaskClient#getOrchestrationHistory(String)}. Each
 * concrete subclass (for example {@link ExecutionStartedEvent} or {@link TaskCompletedEvent}) exposes the data specific
 * to that event type. Use {@code instanceof} to inspect the concrete event type.
 */
public abstract class HistoryEvent {
    private final int eventId;
    private final Instant timestamp;

    HistoryEvent(int eventId, Instant timestamp) {
        this.eventId = eventId;
        this.timestamp = timestamp;
    }

    /**
     * Gets the sequence ID of this history event, or {@code -1} if the event is not associated with a specific action.
     *
     * @return the event sequence ID
     */
    public int getEventId() {
        return this.eventId;
    }

    /**
     * Gets the UTC timestamp at which this history event was recorded.
     *
     * @return the event timestamp
     */
    public Instant getTimestamp() {
        return this.timestamp;
    }
}
