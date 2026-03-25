// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.durabletask.ITaskEntity;
import com.microsoft.durabletask.TaskEntityOperation;

import java.util.Map;

/**
 * A low-level counter entity that uses {@link ITaskEntity} with manual operation dispatch.
 * <p>
 * This mirrors the .NET {@code ManualCounter} from {@code Counter.cs} and demonstrates
 * how to process entity operations without the convenience of {@link com.microsoft.durabletask.TaskEntity}.
 */
public class ManualCounterEntity implements ITaskEntity {

    @Override
    public Object run(TaskEntityOperation operation) {
        if (operation.getState().getState(Integer.class) == null) {
            operation.getState().setState(0);
        }

        switch (operation.getName().toLowerCase()) {
            case "add":
                int state = operation.getState().getState(Integer.class);
                state += operation.getInput(int.class);
                operation.getState().setState(state);
                return state;
            case "reset":
                operation.getState().setState(0);
                return null;
            case "get":
                Integer current = operation.getState().getState(Integer.class);
                return current != null ? current : 0;
            case "delete":
                operation.getState().setState(null);
                return null;
            default:
                return null;
        }
    }
}
