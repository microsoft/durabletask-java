// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Functional interface for creating {@link TaskEntity} instances.
 * <p>
 * Entity factories are registered with the {@link DurableTaskGrpcWorkerBuilder} and are used to create
 * new entity instances when entity work items are received from the sidecar.
 */
@FunctionalInterface
public interface TaskEntityFactory {
    /**
     * Creates a new instance of {@link TaskEntity}.
     *
     * @return a new entity instance
     */
    TaskEntity create();
}
