// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents an asynchronous operation in a durable orchestration.
 * <p>
 * {@code Task<V>} instances are created by methods on the {@link TaskOrchestrationContext} class, which is available
 * in {@link TaskOrchestration} implementations. For example, scheduling an activity will return a task.
 * <pre>
 * Task{@literal <}int{@literal >} activityTask = ctx.callActivity("MyActivity", someInput, int.class);
 * </pre>
 * <p>
 * Orchestrator code uses the {@link #await()} method to block on the completion of the task and retrieve the result.
 * If the task is not yet complete, the {@code await()} method will throw an {@link OrchestratorBlockedEvent}, which
 * pauses the orchestrator's execution so that it can save its progress into durable storage and schedule any
 * outstanding work. When the task is complete, the orchestrator will run again from the beginning and the next time
 * the task's {@code await()} method is called, the result will be returned, or a {@link TaskFailedException} will be
 * thrown if the result of the task was an unhandled exception.
 * <p>
 * Note that orchestrator code must never catch {@code OrchestratorBlockedEvent} because doing so can cause the
 * orchestration instance to get permanently stuck.
 *
 * @param <V> the return type of the task
 */
public abstract class Task<V> {
    final CompletableFuture<V> future;

    Task(CompletableFuture<V> future) {
        this.future = future;
    }

    /**
     * Returns {@code true} if completed in any fashion: normally, with an exception, or via cancellation.
     * @return {@code true} if completed, otherwise {@code false}
     */
    public boolean isDone() {
        return this.future.isDone();
    }

    /**
     * Returns {@code true} if the task was cancelled.
     * @return {@code true} if the task was cancelled, otherwise {@code false}
     */
    public boolean isCancelled() { 
        return this.future.isCancelled();
    }

    /**
     * Blocks the orchestrator until this task to complete, and then returns its result.
     *
     * @return the result of the task
     * @throws TaskFailedException if the task failed with an unhandled exception
     * @throws OrchestratorBlockedEvent if the task has not yet been scheduled, which is a normal occurrence.
     *                                  This {@code Throwable} must never be caught in user code.
     */
    public abstract V await() throws TaskFailedException, OrchestratorBlockedEvent;
}