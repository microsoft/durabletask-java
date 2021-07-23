// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public interface TaskOrchestrationContext {
    String getName();
    <V> V getInput(Class<V> targetType);
    String getInstanceId();
    Instant getCurrentInstant();
    boolean getIsReplaying();
    <V> Task<V> completedTask(V value);
    <V> Task<List<V>> allOf(List<Task<V>> tasks);
    <V> Task<V> callActivity(String name, Object input, Class<V> returnType);
    Task<Void> createTimer(Duration delay);
    void complete(Object output);
    void fail(Object errorOutput);

    default Task<Void> callActivity(String name) {
        return this.callActivity(name, null);
    }

    default Task<Void> callActivity(String name, Object input) {
        return this.callActivity(name, input, Void.class);
    }

}
