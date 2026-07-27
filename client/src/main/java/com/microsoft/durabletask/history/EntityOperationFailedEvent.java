// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import com.microsoft.durabletask.FailureDetails;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when a durable entity operation fails.
 */
public final class EntityOperationFailedEvent extends HistoryEvent {
    private final String requestId;
    private final FailureDetails failureDetails;

    /**
     * Creates a new {@code EntityOperationFailedEvent}.
     *
     * @param eventId        the event sequence ID
     * @param timestamp      the event timestamp
     * @param requestId      the unique request ID of the entity operation
     * @param failureDetails the failure details, or {@code null}
     */
    public EntityOperationFailedEvent(
            int eventId, Instant timestamp, String requestId, @Nullable FailureDetails failureDetails) {
        super(eventId, timestamp);
        this.requestId = requestId;
        this.failureDetails = failureDetails;
    }

    /** @return the unique request ID of the entity operation. */
    public String getRequestId() {
        return this.requestId;
    }

    /** @return the failure details, or {@code null} if not available. */
    @Nullable
    public FailureDetails getFailureDetails() {
        return this.failureDetails;
    }
}
