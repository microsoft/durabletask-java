// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.TaskEntityOperation;

import java.util.Random;
import java.util.UUID;

/**
 * Entity that demonstrates the lifecycle of a durable entity.
 * <p>
 * An entity is initialized on the first operation it receives and is considered deleted when
 * its state is {@code null} at the end of an operation. This mirrors the .NET
 * {@code Lifetime} entity from {@code Lifetime.cs}.
 * <p>
 * Key concepts demonstrated:
 * <ul>
 *   <li>{@link #initializeState} — customizes the default state for a new entity</li>
 *   <li>{@link #getAllowStateDispatch} — controls whether operations can be dispatched to the state object</li>
 *   <li>{@link #customDelete} — shows that deletion can happen from any operation by nulling state</li>
 *   <li>Implicit {@code delete} — handled by the base class when no matching method is found</li>
 * </ul>
 */
public class LifetimeEntity extends AbstractTaskEntity<LifetimeState> {

    private static final Random RANDOM = new Random();

    /**
     * Returns the current entity state.
     */
    public LifetimeState get() {
        return this.state;
    }

    /**
     * No-op operation that simply initializes the entity if it doesn't already exist.
     */
    public void init() {
        // No-op — just triggers entity initialization via initializeState
    }

    /**
     * Demonstrates that entity deletion can be accomplished from any operation by nulling out the state.
     * The operation doesn't have to be named "delete" — the only requirement for deletion is that
     * state is {@code null} when the operation returns.
     */
    public void customDelete() {
        this.state = null;
    }

    /**
     * Explicitly handles the "delete" operation.
     * <p>
     * Entities have an implicit "delete" operation when there is no matching "delete" method.
     * By explicitly adding a delete method, it overrides the implicit behavior.
     * Since state deletion is determined by nulling {@code this.state}, value-types cannot be
     * deleted except by the implicit delete (which will still delete them).
     */
    public void delete() {
        this.state = null;
    }

    @Override
    protected LifetimeState initializeState(TaskEntityOperation operation) {
        // Customizes the default state value for a new entity
        return new LifetimeState(
                UUID.randomUUID().toString().replace("-", ""),
                RANDOM.nextInt(1000));
    }

    @Override
    protected Class<LifetimeState> getStateType() {
        return LifetimeState.class;
    }
}
