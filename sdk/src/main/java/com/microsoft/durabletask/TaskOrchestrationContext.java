// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

    <V> Task<V> callActivity(String name, Object input, TaskOptions options, Class<V> returnType);

    default Task<Void> callActivity(String name) {
        return this.callActivity(name, null);
    }

    default Task<Void> callActivity(String name, Object input) {
        return this.callActivity(name, input, null, Void.class);
    }

    default <V> Task<V> callActivity(String name, Object input, Class<V> returnType) {
        return this.callActivity(name, input, null, returnType);
    }

    default Task<Void> callActivity(String name, Object input, TaskOptions options) {
        return this.callActivity(name, input, options, Void.class);
    }

    default void continueAsNew(Object input){
        this.continueAsNew(input, true);
    }

    void continueAsNew(Object input, boolean preserveUnprocessedEvents );

    default void sendEvent(String instanceId, String eventName){
        this.sendEvent(instanceId, eventName, null);
    }

    void sendEvent(String instanceId, String eventName, Object eventData);

    default Task<Void> callSubOrchestrator(String name){
        return this.callSubOrchestrator(name, null);
    }

    default Task<Void> callSubOrchestrator(String name, Object input) {
        return this.callSubOrchestrator(name, input, null);
    }

    default <V>Task<V> callSubOrchestrator(String name, Object input, Class<V> returnType) {
        return this.callSubOrchestrator(name, input, null, returnType);
    }

    default <V> Task<V> callSubOrchestrator(String name, Object input, String instanceId, Class<V> returnType) {
        return this.callSubOrchestrator(name, input, instanceId, null, returnType);
    }

    default Task<Void> callSubOrchestrator(String name, Object input, String instanceId, TaskOptions options) {
        return this.callSubOrchestrator(name, input, instanceId, options, Void.class);
    }

    <V> Task<V> callSubOrchestrator(
            String name,
            @Nullable Object input,
            @Nullable String instanceId,
            @Nullable TaskOptions options,
            Class<V> returnType);

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
