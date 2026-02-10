// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides contextual information and side-effecting capabilities to a durable entity during operation execution.
 * <p>
 * The context allows entities to:
 * <ul>
 *   <li>Get their own entity instance ID</li>
 *   <li>Signal other entities (fire-and-forget)</li>
 *   <li>Start new orchestration instances</li>
 * </ul>
 * <p>
 * Actions performed through the context (signals and orchestration starts) are collected and
 * submitted as part of the entity batch result. They support transactional semantics:
 * if an operation fails, any actions enqueued during that operation are rolled back.
 */
public abstract class TaskEntityContext {
    /**
     * Gets the instance ID of the currently executing entity.
     *
     * @return the entity instance ID
     */
    @Nonnull
    public abstract EntityInstanceId getId();

    /**
     * Sends a fire-and-forget signal to another entity.
     *
     * @param entityId      the target entity's instance ID
     * @param operationName the name of the operation to invoke on the target entity
     */
    public void signalEntity(@Nonnull EntityInstanceId entityId, @Nonnull String operationName) {
        this.signalEntity(entityId, operationName, null, null);
    }

    /**
     * Sends a fire-and-forget signal to another entity with input data.
     *
     * @param entityId      the target entity's instance ID
     * @param operationName the name of the operation to invoke on the target entity
     * @param input         the input data for the operation, or {@code null}
     */
    public void signalEntity(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nullable Object input) {
        this.signalEntity(entityId, operationName, input, null);
    }

    /**
     * Sends a fire-and-forget signal to another entity with input data and options.
     *
     * @param entityId      the target entity's instance ID
     * @param operationName the name of the operation to invoke on the target entity
     * @param input         the input data for the operation, or {@code null}
     * @param options       signal options (e.g., scheduled time), or {@code null}
     */
    public abstract void signalEntity(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nullable Object input,
            @Nullable SignalEntityOptions options);

    /**
     * Starts a new orchestration instance.
     *
     * @param name  the name of the orchestration to start
     * @param input the input for the orchestration, or {@code null}
     * @return the instance ID of the newly started orchestration
     */
    @Nonnull
    public String startNewOrchestration(@Nonnull String name, @Nullable Object input) {
        return this.startNewOrchestration(name, input, null);
    }

    /**
     * Starts a new orchestration instance with options.
     *
     * @param name    the name of the orchestration to start
     * @param input   the input for the orchestration, or {@code null}
     * @param options the orchestration start options (e.g., instance ID), or {@code null}
     * @return the instance ID of the newly started orchestration
     */
    @Nonnull
    public abstract String startNewOrchestration(
            @Nonnull String name,
            @Nullable Object input,
            @Nullable NewOrchestrationInstanceOptions options);
}
