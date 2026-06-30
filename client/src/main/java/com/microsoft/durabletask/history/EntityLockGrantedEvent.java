// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import java.time.Instant;

/**
 * History event recorded when a requested entity lock is granted.
 */
public final class EntityLockGrantedEvent extends HistoryEvent {
    private final String criticalSectionId;

    /**
     * Creates a new {@code EntityLockGrantedEvent}.
     *
     * @param eventId           the event sequence ID
     * @param timestamp         the event timestamp
     * @param criticalSectionId the ID of the critical section whose lock was granted
     */
    public EntityLockGrantedEvent(int eventId, Instant timestamp, String criticalSectionId) {
        super(eventId, timestamp);
        this.criticalSectionId = criticalSectionId;
    }

    /** @return the ID of the critical section whose lock was granted. */
    public String getCriticalSectionId() {
        return this.criticalSectionId;
    }
}
