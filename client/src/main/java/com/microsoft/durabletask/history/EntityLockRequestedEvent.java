// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * History event recorded when an orchestration requests a lock over one or more durable entities.
 */
public final class EntityLockRequestedEvent extends HistoryEvent {
    private final String criticalSectionId;
    private final List<String> lockSet;
    private final int position;
    private final String parentInstanceId;

    /**
     * Creates a new {@code EntityLockRequestedEvent}.
     *
     * @param eventId           the event sequence ID
     * @param timestamp         the event timestamp
     * @param criticalSectionId the ID of the critical section associated with the lock request
     * @param lockSet           the ordered set of entity instance IDs being locked, or {@code null}
     * @param position          the position of this entity within the lock set
     * @param parentInstanceId  the requesting instance ID, or {@code null}
     */
    public EntityLockRequestedEvent(
            int eventId,
            Instant timestamp,
            String criticalSectionId,
            @Nullable List<String> lockSet,
            int position,
            @Nullable String parentInstanceId) {
        super(eventId, timestamp);
        this.criticalSectionId = criticalSectionId;
        this.lockSet = lockSet != null
                ? Collections.unmodifiableList(new ArrayList<>(lockSet)) : Collections.emptyList();
        this.position = position;
        this.parentInstanceId = parentInstanceId;
    }

    /** @return the ID of the critical section associated with the lock request. */
    public String getCriticalSectionId() {
        return this.criticalSectionId;
    }

    /** @return the ordered set of entity instance IDs being locked (never {@code null}). */
    public List<String> getLockSet() {
        return this.lockSet;
    }

    /** @return the position of this entity within the lock set. */
    public int getPosition() {
        return this.position;
    }

    /** @return the requesting instance ID, or {@code null} if not set. */
    @Nullable
    public String getParentInstanceId() {
        return this.parentInstanceId;
    }
}
