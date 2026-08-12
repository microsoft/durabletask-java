// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import java.time.Instant;

/**
 * History event recorded at the end of each orchestration replay/episode. Carries no payload.
 */
public final class OrchestratorCompletedEvent extends HistoryEvent {
    /**
     * Creates a new {@code OrchestratorCompletedEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     */
    public OrchestratorCompletedEvent(int eventId, Instant timestamp) {
        super(eventId, timestamp);
    }
}
