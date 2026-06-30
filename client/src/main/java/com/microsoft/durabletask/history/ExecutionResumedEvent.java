// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when a suspended orchestration instance is resumed.
 */
public final class ExecutionResumedEvent extends HistoryEvent {
    private final String input;

    /**
     * Creates a new {@code ExecutionResumedEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     * @param input     the serialized resume reason, or {@code null}
     */
    public ExecutionResumedEvent(int eventId, Instant timestamp, @Nullable String input) {
        super(eventId, timestamp);
        this.input = input;
    }

    /** @return the serialized resume reason, or {@code null} if none. */
    @Nullable
    public String getInput() {
        return this.input;
    }
}
