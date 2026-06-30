// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when an orchestration restarts itself via continue-as-new.
 */
public final class ContinueAsNewEvent extends HistoryEvent {
    private final String input;

    /**
     * Creates a new {@code ContinueAsNewEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     * @param input     the serialized input for the new generation, or {@code null}
     */
    public ContinueAsNewEvent(int eventId, Instant timestamp, @Nullable String input) {
        super(eventId, timestamp);
        this.input = input;
    }

    /** @return the serialized input for the new generation, or {@code null} if none. */
    @Nullable
    public String getInput() {
        return this.input;
    }
}
