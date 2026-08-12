// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event that carries a generic, free-form data payload.
 */
public final class GenericEvent extends HistoryEvent {
    private final String data;

    /**
     * Creates a new {@code GenericEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     * @param data      the serialized event data, or {@code null}
     */
    public GenericEvent(int eventId, Instant timestamp, @Nullable String data) {
        super(eventId, timestamp);
        this.data = data;
    }

    /** @return the serialized event data, or {@code null} if none. */
    @Nullable
    public String getData() {
        return this.data;
    }
}
