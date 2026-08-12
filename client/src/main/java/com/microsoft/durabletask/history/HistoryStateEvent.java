// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event that carries a snapshot of the orchestration's runtime state.
 * <p>
 * This is an internal checkpoint marker. The full state snapshot is surfaced via {@link #getState()}, matching the
 * sibling .NET SDK ({@code HistoryStateEvent.State}) and Python SDK ({@code HistoryStateEvent.orchestration_state}).
 */
public final class HistoryStateEvent extends HistoryEvent {
    private final OrchestrationState state;

    /**
     * Creates a new {@code HistoryStateEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     * @param state     the orchestration state snapshot, or {@code null} if not available
     */
    public HistoryStateEvent(int eventId, Instant timestamp, @Nullable OrchestrationState state) {
        super(eventId, timestamp);
        this.state = state;
    }

    /** @return the orchestration state snapshot, or {@code null} if not available. */
    @Nullable
    public OrchestrationState getState() {
        return this.state;
    }
}
