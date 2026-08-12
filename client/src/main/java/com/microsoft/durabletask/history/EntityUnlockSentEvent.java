// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * History event recorded when an entity lock is released (unlock sent).
 */
public final class EntityUnlockSentEvent extends HistoryEvent {
    private final String criticalSectionId;
    private final String parentInstanceId;
    private final String targetInstanceId;

    /**
     * Creates a new {@code EntityUnlockSentEvent}.
     *
     * @param eventId           the event sequence ID
     * @param timestamp         the event timestamp
     * @param criticalSectionId the ID of the critical section being released
     * @param parentInstanceId  the releasing instance ID, or {@code null}
     * @param targetInstanceId  the target entity instance ID, or {@code null}
     */
    public EntityUnlockSentEvent(
            int eventId,
            Instant timestamp,
            String criticalSectionId,
            @Nullable String parentInstanceId,
            @Nullable String targetInstanceId) {
        super(eventId, timestamp);
        this.criticalSectionId = criticalSectionId;
        this.parentInstanceId = parentInstanceId;
        this.targetInstanceId = targetInstanceId;
    }

    /** @return the ID of the critical section being released. */
    public String getCriticalSectionId() {
        return this.criticalSectionId;
    }

    /** @return the releasing instance ID, or {@code null} if not set. */
    @Nullable
    public String getParentInstanceId() {
        return this.parentInstanceId;
    }

    /** @return the target entity instance ID, or {@code null} if not set. */
    @Nullable
    public String getTargetInstanceId() {
        return this.targetInstanceId;
    }
}
