// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when an external event is delivered to an orchestration instance.
 */
public final class EventRaisedEvent extends HistoryEvent {
    private final String name;
    private final String input;

    /**
     * Creates a new {@code EventRaisedEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     * @param name      the name of the event
     * @param input     the serialized event payload, or {@code null}
     */
    public EventRaisedEvent(int eventId, Instant timestamp, String name, @Nullable String input) {
        super(eventId, timestamp);
        this.name = name;
        this.input = input;
    }

    /** @return the name of the event. */
    public String getName() {
        return this.name;
    }

    /** @return the serialized event payload, or {@code null} if none. */
    @Nullable
    public String getInput() {
        return this.input;
    }
}
