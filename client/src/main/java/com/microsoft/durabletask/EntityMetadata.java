// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Represents metadata about a durable entity instance, including its identity, state, and lock status.
 */
public final class EntityMetadata {
    private final String instanceId;
    private final Instant lastModifiedTime;
    private final int backlogQueueSize;
    private final String lockedBy;
    private final String serializedState;
    private final DataConverter dataConverter;

    /**
     * Creates a new {@code EntityMetadata} instance.
     *
     * @param instanceId       the entity instance ID string (in {@code @name@key} format)
     * @param lastModifiedTime the time the entity was last modified
     * @param backlogQueueSize the number of operations waiting in the entity's backlog queue
     * @param lockedBy         the orchestration instance ID that currently holds a lock on this entity, or {@code null}
     * @param serializedState  the serialized entity state, or {@code null} if state was not fetched
     * @param dataConverter    the data converter used to deserialize state
     */
    EntityMetadata(
            String instanceId,
            Instant lastModifiedTime,
            int backlogQueueSize,
            @Nullable String lockedBy,
            @Nullable String serializedState,
            DataConverter dataConverter) {
        this.instanceId = instanceId;
        this.lastModifiedTime = lastModifiedTime;
        this.backlogQueueSize = backlogQueueSize;
        this.lockedBy = lockedBy;
        this.serializedState = serializedState;
        this.dataConverter = dataConverter;
    }

    /**
     * Gets the entity instance ID string.
     *
     * @return the instance ID
     */
    public String getInstanceId() {
        return this.instanceId;
    }

    /**
     * Gets the parsed {@link EntityInstanceId} from the instance ID string.
     *
     * @return the parsed entity instance ID
     */
    public EntityInstanceId getEntityInstanceId() {
        return EntityInstanceId.fromString(this.instanceId);
    }

    /**
     * Gets the time the entity was last modified.
     *
     * @return the last modified time
     */
    public Instant getLastModifiedTime() {
        return this.lastModifiedTime;
    }

    /**
     * Gets the number of operations waiting in the entity's backlog queue.
     *
     * @return the backlog queue size
     */
    public int getBacklogQueueSize() {
        return this.backlogQueueSize;
    }

    /**
     * Gets the orchestration instance ID that currently holds a lock on this entity,
     * or {@code null} if the entity is not locked.
     *
     * @return the locking orchestration instance ID, or {@code null}
     */
    @Nullable
    public String getLockedBy() {
        return this.lockedBy;
    }

    /**
     * Gets the raw serialized entity state, or {@code null} if state was not fetched.
     *
     * @return the serialized state string, or {@code null}
     */
    @Nullable
    public String getSerializedState() {
        return this.serializedState;
    }

    /**
     * Deserializes the entity state into an object of the specified type.
     *
     * @param stateType the class to deserialize the state into
     * @param <T>       the target type
     * @return the deserialized state, or {@code null} if no state is available
     */
    @Nullable
    public <T> T readStateAs(Class<T> stateType) {
        if (this.serializedState == null) {
            return null;
        }
        return this.dataConverter.deserialize(this.serializedState, stateType);
    }
}
