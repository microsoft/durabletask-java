// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;

/**
 * An extension of {@link EntityMetadata} that provides typed access to the entity's state.
 * <p>
 * This mirrors the .NET SDK's {@code EntityMetadata<TState>} which inherits from {@code EntityMetadata}
 * and provides a typed {@code State} property. In Java, the state is eagerly deserialized and accessible
 * via {@link #getState()}.
 *
 * <h3>Example:</h3>
 * <pre>{@code
 * TypedEntityMetadata<Integer> metadata = client.getEntities()
 *     .getEntityMetadata(entityId, Integer.class);
 * if (metadata != null) {
 *     Integer state = metadata.getState();
 *     System.out.println("Counter value: " + state);
 * }
 * }</pre>
 *
 * @param <T> the type of the entity's state
 * @see EntityMetadata
 * @see DurableEntityClient#getEntityMetadata(EntityInstanceId, Class)
 */
public final class TypedEntityMetadata<T> extends EntityMetadata {

    private final T state;
    private final Class<T> stateType;

    /**
     * Creates a new {@code TypedEntityMetadata} from an existing {@link EntityMetadata} and a state type.
     * <p>
     * The state is eagerly deserialized from the metadata's serialized state.
     *
     * @param source    the source metadata to wrap
     * @param stateType the class to deserialize the state into
     */
    TypedEntityMetadata(EntityMetadata source, Class<T> stateType) {
        super(
                source.getInstanceId(),
                source.getLastModifiedTime(),
                source.getBacklogQueueSize(),
                source.getLockedBy(),
                source.getSerializedState(),
                source.isIncludesState(),
                source.getDataConverter());
        this.stateType = stateType;
        this.state = source.readStateAs(stateType);
    }

    /**
     * Gets the deserialized entity state.
     * <p>
     * Returns {@code null} if the entity has no state or if state was not included in the query.
     *
     * @return the deserialized state, or {@code null}
     */
    @Nullable
    public T getState() {
        return this.state;
    }

    /**
     * Gets the state type class used for deserialization.
     *
     * @return the state type class
     */
    public Class<T> getStateType() {
        return this.stateType;
    }
}
