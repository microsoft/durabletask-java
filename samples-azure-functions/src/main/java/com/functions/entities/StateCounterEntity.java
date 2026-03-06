// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.durabletask.TaskEntity;
import com.microsoft.durabletask.TaskEntityOperation;

/**
 * A counter entity that uses state dispatch (POJO pattern).
 * <p>
 * In this pattern, the state object itself is the entity. All operations are dispatched
 * to public methods on the {@link StateCounterState} POJO. This mirrors the .NET
 * {@code StateCounter} from {@code Counter.cs}.
 * <p>
 * Note the structural difference between {@link CounterEntity} and {@code StateCounterEntity}:
 * <ul>
 *   <li>{@code CounterEntity}: state is {@code Integer} — serialized as just a number</li>
 *   <li>{@code StateCounterEntity}: state is {@link StateCounterState} — serialized as
 *       {@code {"value": number}}</li>
 * </ul>
 */
public class StateCounterEntity extends TaskEntity<StateCounterState> {

    public StateCounterEntity() {
        // Enable state dispatch so operations are forwarded to methods on StateCounterState
        this.setAllowStateDispatch(true);
    }

    @Override
    protected StateCounterState initializeState(TaskEntityOperation operation) {
        return new StateCounterState();
    }

    @Override
    protected Class<StateCounterState> getStateType() {
        return StateCounterState.class;
    }
}
