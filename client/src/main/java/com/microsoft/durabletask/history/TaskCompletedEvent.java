// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when a scheduled activity task completes successfully.
 */
public final class TaskCompletedEvent extends HistoryEvent {
    private final int taskScheduledId;
    private final String result;

    /**
     * Creates a new {@code TaskCompletedEvent}.
     *
     * @param eventId         the event sequence ID
     * @param timestamp       the event timestamp
     * @param taskScheduledId the event ID of the corresponding {@link TaskScheduledEvent}
     * @param result          the serialized activity result, or {@code null}
     */
    public TaskCompletedEvent(int eventId, Instant timestamp, int taskScheduledId, @Nullable String result) {
        super(eventId, timestamp);
        this.taskScheduledId = taskScheduledId;
        this.result = result;
    }

    /** @return the event ID of the corresponding {@link TaskScheduledEvent}. */
    public int getTaskScheduledId() {
        return this.taskScheduledId;
    }

    /** @return the serialized activity result, or {@code null} if none. */
    @Nullable
    public String getResult() {
        return this.result;
    }
}
