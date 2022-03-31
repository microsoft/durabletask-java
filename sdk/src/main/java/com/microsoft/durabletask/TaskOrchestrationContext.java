// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public interface TaskOrchestrationContext {
    String getName();
    <V> V getInput(Class<V> targetType);
    String getInstanceId();
    Instant getCurrentInstant();
    boolean getIsReplaying();
    <V> Task<V> completedTask(V value);
    <V> Task<List<V>> allOf(List<Task<V>> tasks);
    Task<Task<?>> anyOf(List<Task<?>> tasks);

    default Task<Task<?>> anyOf(Task<?>... tasks) {
        return this.anyOf(Arrays.asList(tasks));
    }

    Task<Void> createTimer(Duration delay);
    void complete(Object output);
    void fail(FailureDetails failureDetails);

    <V> Task<V> callActivity(String name, Object input, Class<V> returnType);

    default Task<Void> callActivity(String name) {
        return this.callActivity(name, null);
    }

    default Task<Void> callActivity(String name, Object input) {
        return this.callActivity(name, input, Void.class);
    }

    <V> Task<V> waitForExternalEvent(String name, Duration timeout, Class<V> dataType) throws TaskCanceledException;

    default Task<Void> waitForExternalEvent(String name, Duration timeout) throws TaskCanceledException {
        return this.waitForExternalEvent(name, timeout, Void.class);
    }

    default Task<Void> waitForExternalEvent(String name) {
        return this.waitForExternalEvent(name, Void.class);
    }

    default <V> Task<V> waitForExternalEvent(String name, Class<V> dataType) {
        try {
            return this.waitForExternalEvent(name, null, dataType);
        } catch (TaskCanceledException e) {
            // This should never happen because of the max duration
            throw new RuntimeException("An unexpected exception was throw while waiting for an external event.", e);
        }
    }
}
