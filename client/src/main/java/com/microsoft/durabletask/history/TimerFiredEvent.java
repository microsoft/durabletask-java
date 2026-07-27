// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import java.time.Instant;

/**
 * History event recorded when a durable timer fires.
 */
public final class TimerFiredEvent extends HistoryEvent {
    private final Instant fireAt;
    private final int timerId;

    /**
     * Creates a new {@code TimerFiredEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     * @param fireAt    the time at which the timer was scheduled to fire
     * @param timerId   the event ID of the corresponding {@link TimerCreatedEvent}
     */
    public TimerFiredEvent(int eventId, Instant timestamp, Instant fireAt, int timerId) {
        super(eventId, timestamp);
        this.fireAt = fireAt;
        this.timerId = timerId;
    }

    /** @return the time at which the timer was scheduled to fire. */
    public Instant getFireAt() {
        return this.fireAt;
    }

    /** @return the event ID of the corresponding {@link TimerCreatedEvent}. */
    public int getTimerId() {
        return this.timerId;
    }
}
