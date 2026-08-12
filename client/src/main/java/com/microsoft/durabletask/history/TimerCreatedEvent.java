// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import java.time.Instant;

/**
 * History event recorded when a durable timer is created by an orchestration.
 */
public final class TimerCreatedEvent extends HistoryEvent {
    private final Instant fireAt;

    /**
     * Creates a new {@code TimerCreatedEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     * @param fireAt    the time at which the timer is scheduled to fire
     */
    public TimerCreatedEvent(int eventId, Instant timestamp, Instant fireAt) {
        super(eventId, timestamp);
        this.fireAt = fireAt;
    }

    /** @return the time at which the timer is scheduled to fire. */
    public Instant getFireAt() {
        return this.fireAt;
    }
}
