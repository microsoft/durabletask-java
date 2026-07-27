// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import java.time.Instant;

/**
 * History event recorded at the start of each orchestration replay/episode. Carries no payload.
 */
public final class OrchestratorStartedEvent extends HistoryEvent {
    /**
     * Creates a new {@code OrchestratorStartedEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     */
    public OrchestratorStartedEvent(int eventId, Instant timestamp) {
        super(eventId, timestamp);
    }
}
