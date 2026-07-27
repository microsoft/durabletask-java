// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when a durable entity operation completes successfully.
 */
public final class EntityOperationCompletedEvent extends HistoryEvent {
    private final String requestId;
    private final String output;

    /**
     * Creates a new {@code EntityOperationCompletedEvent}.
     *
     * @param eventId   the event sequence ID
     * @param timestamp the event timestamp
     * @param requestId the unique request ID of the entity operation
     * @param output    the serialized operation output, or {@code null}
     */
    public EntityOperationCompletedEvent(
            int eventId, Instant timestamp, String requestId, @Nullable String output) {
        super(eventId, timestamp);
        this.requestId = requestId;
        this.output = output;
    }

    /** @return the unique request ID of the entity operation. */
    public String getRequestId() {
        return this.requestId;
    }

    /** @return the serialized operation output, or {@code null} if none. */
    @Nullable
    public String getOutput() {
        return this.output;
    }
}
