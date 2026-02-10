// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Common interface for durable entity implementations.
 * <p>
 * Entities are stateful, single-threaded actors identified by a name+key pair
 * ({@link EntityInstanceId}). The durable task runtime manages state persistence,
 * message routing, and concurrency control.
 * <p>
 * Implement this interface directly for full control over entity behavior, or extend
 * {@link TaskEntity} for a higher-level programming model with automatic reflection-based
 * operation dispatch.
 *
 * @see TaskEntity
 */
@FunctionalInterface
public interface ITaskEntity {
    /**
     * Executes the entity logic for a single operation.
     *
     * @param operation the operation to execute, including the operation name, input, state, and context
     * @return the result of the operation, which will be serialized and returned to the caller
     *         (for two-way calls). May be {@code null} for void operations.
     * @throws Exception if the operation fails
     */
    Object runAsync(TaskEntityOperation operation) throws Exception;
}
