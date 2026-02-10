// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a single operation to be executed against a durable entity.
 * <p>
 * An operation includes:
 * <ul>
 *   <li>The operation name (e.g., "add", "get", "delete")</li>
 *   <li>Optional input data</li>
 *   <li>Access to the entity's state via {@link TaskEntityState}</li>
 *   <li>Access to the entity's context via {@link TaskEntityContext}</li>
 * </ul>
 */
public class TaskEntityOperation {
    private final String name;
    private final String serializedInput;
    private final TaskEntityContext context;
    private final TaskEntityState state;
    private final DataConverter dataConverter;

    /**
     * Creates a new {@code TaskEntityOperation}.
     *
     * @param name            the operation name
     * @param serializedInput the serialized input data, or {@code null} if no input
     * @param context         the entity context
     * @param state           the entity state
     * @param dataConverter   the data converter for deserializing input
     */
    public TaskEntityOperation(
            @Nonnull String name,
            @Nullable String serializedInput,
            @Nonnull TaskEntityContext context,
            @Nonnull TaskEntityState state,
            @Nonnull DataConverter dataConverter) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Operation name must not be null or empty.");
        }
        this.name = name;
        this.serializedInput = serializedInput;
        this.context = context;
        this.state = state;
        this.dataConverter = dataConverter;
    }

    /**
     * Gets the name of this operation.
     *
     * @return the operation name
     */
    @Nonnull
    public String getName() {
        return this.name;
    }

    /**
     * Deserializes and returns the input data for this operation.
     *
     * @param inputType the class to deserialize the input into
     * @param <T>       the expected type of the input
     * @return the deserialized input, or {@code null} if no input was provided
     */
    @Nullable
    public <T> T getInput(@Nonnull Class<T> inputType) {
        if (this.serializedInput == null) {
            return null;
        }
        return this.dataConverter.deserialize(this.serializedInput, inputType);
    }

    /**
     * Returns whether this operation has input data.
     *
     * @return {@code true} if input data was provided, {@code false} otherwise
     */
    public boolean hasInput() {
        return this.serializedInput != null;
    }

    /**
     * Gets the context for the currently executing entity.
     *
     * @return the entity context
     */
    @Nonnull
    public TaskEntityContext getContext() {
        return this.context;
    }

    /**
     * Gets the state accessor for the currently executing entity.
     *
     * @return the entity state
     */
    @Nonnull
    public TaskEntityState getState() {
        return this.state;
    }
}
