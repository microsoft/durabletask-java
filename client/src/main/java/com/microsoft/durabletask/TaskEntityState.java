// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;

/**
 * Provides access to the state of a durable entity during operation execution.
 * <p>
 * Entity state is automatically persisted by the durable task runtime after each successfully
 * committed operation. The state supports transactional semantics: if an operation fails,
 * the state is rolled back to the last committed snapshot.
 */
public class TaskEntityState {
    private final DataConverter dataConverter;
    private String serializedState;
    private boolean stateExists;

    // Transactional rollback support
    private String committedSerializedState;
    private boolean committedStateExists;

    /**
     * Creates a new {@code TaskEntityState} instance.
     *
     * @param dataConverter   the data converter used for serialization/deserialization
     * @param serializedState the initial serialized state, or {@code null} if no state exists
     */
    public TaskEntityState(DataConverter dataConverter, @Nullable String serializedState) {
        this.dataConverter = dataConverter;
        this.serializedState = serializedState;
        this.stateExists = serializedState != null;
        // Initialize committed state to match initial state
        this.committedSerializedState = serializedState;
        this.committedStateExists = this.stateExists;
    }

    /**
     * Gets the current entity state, deserialized to the specified type.
     *
     * @param stateType the class to deserialize the state into
     * @param <T>       the type of the state
     * @return the deserialized state, or {@code null} if no state exists
     */
    @Nullable
    public <T> T getState(Class<T> stateType) {
        if (!this.stateExists || this.serializedState == null) {
            return null;
        }
        return this.dataConverter.deserialize(this.serializedState, stateType);
    }

    /**
     * Sets the entity state. The state will be serialized using the configured {@link DataConverter}.
     *
     * @param state the state value to set
     */
    public void setState(@Nullable Object state) {
        if (state == null) {
            deleteState();
        } else {
            this.serializedState = this.dataConverter.serialize(state);
            this.stateExists = true;
        }
    }

    /**
     * Returns whether the entity currently has state.
     *
     * @return {@code true} if the entity has state, {@code false} otherwise
     */
    public boolean hasState() {
        return this.stateExists;
    }

    /**
     * Deletes the entity state. After calling this method, {@link #hasState()} will return {@code false}.
     */
    public void deleteState() {
        this.serializedState = null;
        this.stateExists = false;
    }

    /**
     * Gets the serialized (raw) state string.
     *
     * @return the serialized state, or {@code null} if no state exists
     */
    @Nullable
    String getSerializedState() {
        return this.serializedState;
    }

    /**
     * Takes a snapshot of the current state as a rollback point.
     * Called before each operation in a batch to support transactional semantics.
     */
    void commit() {
        this.committedSerializedState = this.serializedState;
        this.committedStateExists = this.stateExists;
    }

    /**
     * Reverts the state to the last committed snapshot.
     * Called when an operation fails to maintain transactional semantics.
     */
    void rollback() {
        this.serializedState = this.committedSerializedState;
        this.stateExists = this.committedStateExists;
    }
}
