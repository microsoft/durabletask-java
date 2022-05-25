// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Interface that provides {@link TaskActivity} implementations with activity context, such as an activity's name and
 * its input.
 */
public interface TaskActivityContext {
    /**
     * Gets the name of the current task activity.
     * @return the name of the current task activity
     */
    String getName();

    /**
     * Gets the deserialized activity input.
     *
     * @param targetType the {@link Class<T>} object associated with {@code T}
     * @param <T> the target type to deserialize the input into
     * @return the deserialized activity input value
     */
    <T> T getInput(Class<T> targetType);
}
