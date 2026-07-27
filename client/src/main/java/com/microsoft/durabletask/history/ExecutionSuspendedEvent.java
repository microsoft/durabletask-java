// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when an orchestration instance is suspended.
 */
public final class ExecutionSuspendedEvent extends HistoryEvent {
    private final String input;

    /**
     * Creates a new {@code ExecutionSuspendedEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     * @param input     the serialized suspension reason, or {@code null}
     */
    public ExecutionSuspendedEvent(int eventId, Instant timestamp, @Nullable String input) {
        super(eventId, timestamp);
        this.input = input;
    }

    /** @return the serialized suspension reason, or {@code null} if none. */
    @Nullable
    public String getInput() {
        return this.input;
    }
}
