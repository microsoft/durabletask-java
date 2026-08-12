// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when a one-way (signal) operation is sent to a durable entity.
 */
public final class EntityOperationSignaledEvent extends HistoryEvent {
    private final String requestId;
    private final String operation;
    private final Instant scheduledTime;
    private final String input;
    private final String targetInstanceId;

    /**
     * Creates a new {@code EntityOperationSignaledEvent}.
     *
     * @param eventId          the event sequence ID
     * @param timestamp        the event timestamp
     * @param requestId        the unique request ID of the entity operation
     * @param operation        the name of the entity operation
     * @param scheduledTime    the scheduled delivery time, or {@code null}
     * @param input            the serialized operation input, or {@code null}
     * @param targetInstanceId the target entity instance ID, or {@code null}
     */
    public EntityOperationSignaledEvent(
            int eventId,
            Instant timestamp,
            String requestId,
            String operation,
            @Nullable Instant scheduledTime,
            @Nullable String input,
            @Nullable String targetInstanceId) {
        super(eventId, timestamp);
        this.requestId = requestId;
        this.operation = operation;
        this.scheduledTime = scheduledTime;
        this.input = input;
        this.targetInstanceId = targetInstanceId;
    }

    /** @return the unique request ID of the entity operation. */
    public String getRequestId() {
        return this.requestId;
    }

    /** @return the name of the entity operation. */
    public String getOperation() {
        return this.operation;
    }

    /** @return the scheduled delivery time, or {@code null} if delivered immediately. */
    @Nullable
    public Instant getScheduledTime() {
        return this.scheduledTime;
    }

    /** @return the serialized operation input, or {@code null} if none. */
    @Nullable
    public String getInput() {
        return this.input;
    }

    /** @return the target entity instance ID, or {@code null} if not set. */
    @Nullable
    public String getTargetInstanceId() {
        return this.targetInstanceId;
    }
}
