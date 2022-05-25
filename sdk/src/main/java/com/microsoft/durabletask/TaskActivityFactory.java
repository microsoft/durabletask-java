// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Factory interface for producing {@link TaskActivity} implementations.
 */
public interface TaskActivityFactory {
    /**
     * Gets the name of the activity this factory creates.
     * @return the name of the activity
     */
    String getName();

    /**
     * Creates a new instance of {@link TaskActivity}
     * @return the created activity instance
     */
    TaskActivity create();
}
