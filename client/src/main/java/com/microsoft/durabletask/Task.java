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
 * The {@code Task<V>} class is similar to the {@link CompletableFuture} class in the Java class library, except that
 * instances of {@code Task<V>} are immutable from user code and all callbacks are invoked on the orchestrator's
 * main execution thread. Internally, {@code Task<V>} uses {@code CompletableFuture<T>} in its internal implementation
 * in a way that makes it compatible with orchestrator code constraints.
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

    /**
     * Returns a new {@code Task<V>} that, when this task completes normally, executes the given action.
     *
     * @param fn the action to run before completing the returned {@code Task<V>}
     * @return the new {@code Task<V>}
     */
    public abstract Task<Void> thenRun(Runnable fn);

    /**
     * Returns a new {@code Task<V>} that, when this task completes normally, executes the given action with this task's
     * result as the argument to the supplied action.
     *
     * @param fn the action to run before completing the returned {@code Task<V>}
     * @return the new {@code Task<V>}
     */
    public abstract Task<Void> thenAccept(Consumer<? super V> fn);

    /**
     * Returns a new {@code Task<V>} that, when this task completes normally, executes the given action with this task's
     * result as the argument to the supplied function.
     *
     * @param fn The function to use to compute the value of the returned {@code Task<V>}
     * @param <R> the function's return type
     * @return the new {@code Task<V>}
     */
    public abstract <R> Task<R> thenApply(Function<? super V, ? extends R> fn);

    /**
     * Returns a new {@code Task<V>} that, when this task completes normally, is executed with this task as the argument
     * to the supplied function.
     *
     * @param fn the function returning a new {@code Task<V>}
     * @param <R> the type of the returned {@code Task<V>}'s result
     * @return the new {@code Task<V>}
     */
    public abstract <R> Task<R> thenCompose(Function<? super V, ? extends Task<R>> fn);
}