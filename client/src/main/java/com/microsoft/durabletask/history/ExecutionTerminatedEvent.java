// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when an orchestration instance is terminated.
 */
public final class ExecutionTerminatedEvent extends HistoryEvent {
    private final String input;
    private final boolean recurse;

    /**
     * Creates a new {@code ExecutionTerminatedEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     * @param input     the serialized termination input/reason, or {@code null}
     * @param recurse   whether termination recurses into sub-orchestrations
     */
    public ExecutionTerminatedEvent(int eventId, Instant timestamp, @Nullable String input, boolean recurse) {
        super(eventId, timestamp);
        this.input = input;
        this.recurse = recurse;
    }

    /** @return the serialized termination input/reason, or {@code null} if none. */
    @Nullable
    public String getInput() {
        return this.input;
    }

    /** @return whether termination recurses into sub-orchestrations. */
    public boolean isRecurse() {
        return this.recurse;
    }
}
