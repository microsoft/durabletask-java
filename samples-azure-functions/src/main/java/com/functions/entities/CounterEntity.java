// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.TaskEntityOperation;

/**
 * A simple counter entity that demonstrates the three main dispatch modes:
 * <ul>
 *   <li><b>Entity dispatch</b> (class-based) — extends {@link AbstractTaskEntity} directly</li>
 *   <li><b>State dispatch</b> (POJO) — dispatches operations to the state object itself</li>
 *   <li><b>Manual dispatch</b> (low-level) — uses {@code TaskEntity} for manual operation routing</li>
 * </ul>
 * <p>
 * This sample mirrors the .NET {@code Counter.cs} sample from
 * {@code durabletask-dotnet/samples/AzureFunctionsApp/Entities/}.
 */
public class CounterEntity extends AbstractTaskEntity<Integer> {

    /**
     * Adds the given input to the current counter state and returns the new value.
     */
    public int add(int input) {
        this.state += input;
        return this.state;
    }

    /**
     * Returns the current counter value.
     */
    public int get() {
        return this.state;
    }

    /**
     * Resets the counter to zero.
     */
    public void reset() {
        this.state = 0;
    }

    @Override
    protected Integer initializeState(TaskEntityOperation operation) {
        return 0;
    }

    @Override
    protected Class<Integer> getStateType() {
        return Integer.class;
    }
}
