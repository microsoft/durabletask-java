// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when an orchestration sends an external event to another instance.
 */
public final class EventSentEvent extends HistoryEvent {
    private final String instanceId;
    private final String name;
    private final String input;

    /**
     * Creates a new {@code EventSentEvent}.
     *
     * @param eventId    the event sequence ID
     * @param timestamp  the event timestamp
     * @param instanceId the target instance ID the event was sent to
     * @param name       the name of the event
     * @param input      the serialized event payload, or {@code null}
     */
    public EventSentEvent(int eventId, Instant timestamp, String instanceId, String name, @Nullable String input) {
        super(eventId, timestamp);
        this.instanceId = instanceId;
        this.name = name;
        this.input = input;
    }

    /** @return the target instance ID the event was sent to. */
    public String getInstanceId() {
        return this.instanceId;
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
