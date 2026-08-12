// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when a sub-orchestration instance completes successfully.
 */
public final class SubOrchestrationInstanceCompletedEvent extends HistoryEvent {
    private final int taskScheduledId;
    private final String result;

    /**
     * Creates a new {@code SubOrchestrationInstanceCompletedEvent}.
     *
     * @param eventId         the event sequence ID
     * @param timestamp       the event timestamp
     * @param taskScheduledId the event ID of the corresponding {@link SubOrchestrationInstanceCreatedEvent}
     * @param result          the serialized sub-orchestration result, or {@code null}
     */
    public SubOrchestrationInstanceCompletedEvent(
            int eventId, Instant timestamp, int taskScheduledId, @Nullable String result) {
        super(eventId, timestamp);
        this.taskScheduledId = taskScheduledId;
        this.result = result;
    }

    /** @return the event ID of the corresponding {@link SubOrchestrationInstanceCreatedEvent}. */
    public int getTaskScheduledId() {
        return this.taskScheduledId;
    }

    /** @return the serialized sub-orchestration result, or {@code null} if none. */
    @Nullable
    public String getResult() {
        return this.result;
    }
}
