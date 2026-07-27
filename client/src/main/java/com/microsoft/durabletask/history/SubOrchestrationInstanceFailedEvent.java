// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import com.microsoft.durabletask.FailureDetails;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when a sub-orchestration instance fails.
 */
public final class SubOrchestrationInstanceFailedEvent extends HistoryEvent {
    private final int taskScheduledId;
    private final FailureDetails failureDetails;

    /**
     * Creates a new {@code SubOrchestrationInstanceFailedEvent}.
     *
     * @param eventId         the event sequence ID
     * @param timestamp       the event timestamp
     * @param taskScheduledId the event ID of the corresponding {@link SubOrchestrationInstanceCreatedEvent}
     * @param failureDetails  the failure details, or {@code null}
     */
    public SubOrchestrationInstanceFailedEvent(
            int eventId, Instant timestamp, int taskScheduledId, @Nullable FailureDetails failureDetails) {
        super(eventId, timestamp);
        this.taskScheduledId = taskScheduledId;
        this.failureDetails = failureDetails;
    }

    /** @return the event ID of the corresponding {@link SubOrchestrationInstanceCreatedEvent}. */
    public int getTaskScheduledId() {
        return this.taskScheduledId;
    }

    /** @return the failure details, or {@code null} if not available. */
    @Nullable
    public FailureDetails getFailureDetails() {
        return this.failureDetails;
    }
}
