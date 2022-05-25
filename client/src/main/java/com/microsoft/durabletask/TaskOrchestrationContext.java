// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * Used by orchestrators to perform actions such as scheduling tasks, durable timers, waiting for external events,
 * and for getting basic information about the current orchestration.
 */
public interface TaskOrchestrationContext {
    /**
     * Gets the name of the current task orchestration.
     * @return the name of the current task orchestration
     */
    String getName();

    /**
     * Gets the deserialized input of the current task orchestration.
     *
     * @param targetType the {@link Class} object associated with {@code V}
     * @param <V> the expected type of the orchestrator input
     * @return the deserialized input as an object of type {@code V} or {@code null} if no input was provided.
     */
    <V> V getInput(Class<V> targetType);

    /**
     * Gets the unique ID of the current orchestration instance.
     * @return the unique ID of the current orchestration instance
     */
    String getInstanceId();

    /**
     * Gets the current orchestration time in UTC.
     * @return the current orchestration time in UTC
     */
    Instant getCurrentInstant();

    /**
     * Gets a value indicating whether the orchestrator is currently replaying a previous execution.
     * <p>
     * Orchestrator functions are "replayed" after being unloaded from memory to reconstruct local variable state.
     * During a replay, previously executed tasks will be completed automatically with previously seen values
     * that are stored in the orchestration history. One the orchestrator reaches the point in the orchestrator
     * where it's no longer replaying existing history, this method will return {@code false}.
     * <p>
     * You can use this method if you have logic that needs to run only when <em>not</em> replaying. For example,
     * certain types of application logging may become too noisy when duplicated as part of replay. The
     * application code could check to see whether the function is being replayed and then issue the log statements
     * when this value is {@code false}.
     *
     * @return {@code true} if the orchestrator is replaying, otherwise {@code false}
     */
    boolean getIsReplaying();

    // TODO: Update the description of allOf to be more specific about the exception behavior.
    //       https://github.com/microsoft/durabletask-java/issues/54
    /**
     * Returns a new {@code Task} that is completed when all the given {@code Task}s complete. If any of the given
     * {@code Task}s complete exceptionally, the returned {@code Task} will also complete exceptionally with information
     * about the first encountered failure. The value of the returned {@code Task} is an ordered list of the return
     * values of the given tasks. If no tasks are provided, returns a {@code Task} completed with value {@code null}.
     * <p>
     * This method is useful for awaiting the completion of a set of independent tasks before continuing to the next
     * step in the orchestration, as in the following example:
     * <pre>{@code
     * Task<String> t1 = ctx.callActivity("MyActivity", String.class);
     * Task<String> t2 = ctx.callActivity("MyActivity", String.class);
     * Task<String> t3 = ctx.callActivity("MyActivity", String.class);
     *
     * List<String> orderedResults = ctx.allOf(List.of(t1, t2, t3)).await();
     * }</pre>
     *
     * @param tasks the list of {@code Task} objects
     * @param <V> the return type of the {@code Task} objects
     * @return the values of the completed {@code Task} objects in the same order as the source list
     */
    <V> Task<List<V>> allOf(List<Task<V>> tasks);

    /**
     * Returns a new {@code Task} that is completed when any of the tasks in {@code tasks} completes.
     * See {@link #anyOf(Task[])} for more detailed information.
     *
     * @param tasks the list of {@code Task} objects
     * @return a new {@code Task} that is completed when any of the given {@code Task}s complete
     * @see #anyOf(Task[])
     */
    Task<Task<?>> anyOf(List<Task<?>> tasks);

    /**
     * Returns a new {@code Task} that is completed when any of the given {@code Task}s complete. The value of the
     * new {@code Task} is a reference to the completed {@code Task} object. If no tasks are provided, returns a
     * {@code Task} that never completes.
     * <p>
     * This method is useful for waiting on multiple concurrent tasks and performing a task-specific operation when the
     * first task completes, as in the following example:
     * <pre>{@code
     * Task<Void> event1 = ctx.waitForExternalEvent("Event1");
     * Task<Void> event2 = ctx.waitForExternalEvent("Event2");
     * Task<Void> event3 = ctx.waitForExternalEvent("Event3");
     *
     * Task<?> winner = ctx.anyOf(event1, event2, event3).await();
     * if (winner == event1) {
     *     // ...
     * } else if (winner == event2) {
     *     // ...
     * } else if (winner == event3) {
     *     // ...
     * }
     * }</pre>
     * The {@code anyOf} method can also be used for implementing long-running timeouts, as in the following example:
     * <pre>{@code
     * Task<Void> activityTask = ctx.callActivity("SlowActivity");
     * Task<Void> timeoutTask = ctx.createTimer(Duration.ofMinutes(30));
     *
     * Task<?> winner = ctx.anyOf(activityTask, timeoutTask).await();
     * if (winner == activityTask) {
     *     // completion case
     * } else {
     *     // timeout case
     * }
     * }</pre>
     *
     * @param tasks the list of {@code Task} objects
     * @return a new {@code Task} that is completed when any of the given {@code Task}s complete
     */
    default Task<Task<?>> anyOf(Task<?>... tasks) {
        return this.anyOf(Arrays.asList(tasks));
    }

    /**
     * Creates a durable timer that expires after the specified delay.
     * <p>
     * Specifying a long delay (for example, a delay of a few days or more) may result in the creation of multiple,
     * internally-managed durable timers. The orchestration code doesn't need to be aware of this behavior. However,
     * it may be visible in framework logs and the stored history state.
     *
     * @param delay the amount of time before the timer should expire
     * @return a new {@code Task} that completes after the specified delay
     */
    Task<Void> createTimer(Duration delay);

    /**
     * Transitions the orchestration into the {@link OrchestrationRuntimeStatus#COMPLETED} state with the given output.
     *
     * @param output the serializable output of the completed orchestration
     */
    void complete(Object output);

    /**
     * Asynchronously invokes an activity by name and with the specified input value and returns a new {@link Task}
     * that completes when the activity completes. If the activity completes successfully, the returned {@code Task}'s
     * value will be the activity's output. If the activity fails, the returned {@code Task} will complete exceptionally
     * with a {@link TaskFailedException}.
     * <p>
     * Activities are the basic unit of work in a durable task orchestration. Unlike orchestrators, which are not
     * allowed to do any I/O or call non-deterministic APIs, activities have no implementation restrictions.
     * <p>
     * An activity may execute in the local machine or a remote machine. The exact behavior depends on the underlying
     * storage provider, which is responsible for distributing tasks across machines. In general, you should never make
     * any assumptions about where an activity will run. You should also assume at-least-once execution guarantees for
     * activities, meaning that an activity may be executed twice if, for example, there is a process failure before
     * the activities result is saved into storage.
     * <p>
     * Both the inputs and outputs of activities are serialized and stored in durable storage. It's highly recommended
     * to not include any sensitive data in activity inputs or outputs. It's also recommended to not use large payloads
     * for activity inputs and outputs, which can result in expensive serialization and network utilization. For data
     * that cannot be cheaply or safely persisted to storage, it's recommended to instead pass <em>references</em>
     * (for example, a URL to a storage blog) to the data and have activities fetch the data directly as part of their
     * implementation.
     *
     * @param name the name of the activity to call
     * @param input the serializable input to pass to the activity
     * @param options additional options that control the execution and processing of the activity
     * @param returnType the expected class type of the activity output
     * @param <V> the expected type of the activity output
     * @return a new {@link Task} that completes when the activity completes or fails
     */
    <V> Task<V> callActivity(String name, Object input, TaskOptions options, Class<V> returnType);

    /**
     * Asynchronously invokes an activity by name and returns a new {@link Task} that completes when the activity
     * completes. See {@link #callActivity(String, Object, TaskOptions, Class)} for a complete description.
     *
     * @see #callActivity(String, Object, TaskOptions, Class)
     * @param name the name of the activity to call
     * @return a new {@link Task} that completes when the activity completes or fails
     */
    default Task<Void> callActivity(String name) {
        return this.callActivity(name, Void.class);
    }

    /**
     * Asynchronously invokes an activity by name and with the specified input value and returns a new {@link Task}
     * that completes when the activity completes. See {@link #callActivity(String, Object, TaskOptions, Class)} for a
     * complete description.
     *
     * @param name the name of the activity to call
     * @param input the serializable input to pass to the activity
     * @return a new {@link Task} that completes when the activity completes or fails
     */
    default Task<Void> callActivity(String name, Object input) {
        return this.callActivity(name, input, null, Void.class);
    }

    /**
     * Asynchronously invokes an activity by name and returns a new {@link Task} that completes when the activity
     * completes. If the activity completes successfully, the returned {@code Task}'s value will be the activity's
     * output. See {@link #callActivity(String, Object, TaskOptions, Class)} for a complete description.
     *
     * @param name the name of the activity to call
     * @param returnType the expected class type of the activity output
     * @param <V> the expected type of the activity output
     * @return a new {@link Task} that completes when the activity completes or fails
     */
    default <V> Task<V> callActivity(String name, Class<V> returnType) {
        return this.callActivity(name, null, null, returnType);
    }

    /**
     * Asynchronously invokes an activity by name and with the specified input value and returns a new {@link Task}
     * that completes when the activity completes.If the activity completes successfully, the returned {@code Task}'s
     * value will be the activity's output. See {@link #callActivity(String, Object, TaskOptions, Class)} for a
     * complete description.
     *
     * @param name the name of the activity to call
     * @param input the serializable input to pass to the activity
     * @param returnType the expected class type of the activity output
     * @param <V> the expected type of the activity output
     * @return a new {@link Task} that completes when the activity completes or fails
     */
    default <V> Task<V> callActivity(String name, Object input, Class<V> returnType) {
        return this.callActivity(name, input, null, returnType);
    }

    /**
     * Asynchronously invokes an activity by name and with the specified input value and returns a new {@link Task}
     * that completes when the activity completes. See {@link #callActivity(String, Object, TaskOptions, Class)} for a
     * complete description.
     *
     * @param name the name of the activity to call
     * @param input the serializable input to pass to the activity
     * @param options additional options that control the execution and processing of the activity
     * @return a new {@link Task} that completes when the activity completes or fails
     */
    default Task<Void> callActivity(String name, Object input, TaskOptions options) {
        return this.callActivity(name, input, options, Void.class);
    }

    /**
     * Restarts the orchestration with a new input and clears its history. See {@link #continueAsNew(Object, boolean)}
     * for a full description.
     *
     * @param input the serializable input data to re-initialize the instance with
     */
    default void continueAsNew(Object input){
        this.continueAsNew(input, true);
    }

    /**
     * Restarts the orchestration with a new input and clears its history.
     * <p>
     * This method is primarily designed for eternal orchestrations, which are orchestrations that
     * may not ever complete. It works by restarting the orchestration, providing it with a new input,
     * and truncating the existing orchestration history. It allows an orchestration to continue
     * running indefinitely without having its history grow unbounded. The benefits of periodically
     * truncating history include decreased memory usage, decreased storage volumes, and shorter orchestrator
     * replays when rebuilding state.
     * <p>
     * The results of any incomplete tasks will be discarded when an orchestrator calls {@code continueAsNew}.
     * For example, if a timer is scheduled and then {@code continueAsNew} is called before the timer fires, the timer
     * event will be discarded. The only exception to this is external events. By default, if an external event is
     * received by an orchestration but not yet processed, the event is saved in the orchestration state unit it is
     * received by a call to {@link #waitForExternalEvent}. These events will remain in memory
     * even after an orchestrator restarts using {@code continueAsNew}. This behavior can be disabled by specifying
     * {@code false} for the {@code preserveUnprocessedEvents} parameter value.
     * <p>
     * Orchestrator implementations should complete immediately after calling the{@code continueAsNew} method.
     *
     * @param input the serializable input data to re-initialize the instance with
     * @param preserveUnprocessedEvents {@code true} to push unprocessed external events into the new orchestration
     *                                  history, otherwise {@code false}
     */
    void continueAsNew(Object input, boolean preserveUnprocessedEvents);

    /**
     * Sends an external event to another orchestration instance.
     *
     * @param instanceID the unique ID of the receiving orchestration instance.
     * @param eventName the name of the event to send
     */
    default void sendEvent(String instanceID, String eventName){
        this.sendEvent(instanceID, eventName, null);
    }

    /**
     * Sends an external event to another orchestration instance.
     *
     * @param instanceId the unique ID of the receiving orchestration instance.
     * @param eventName the name of the event to send
     * @param eventData the payload of the event to send
     */
    void sendEvent(String instanceId, String eventName, Object eventData);

    /**
     * Asynchronously invokes another orchestrator as a sub-orchestration and returns a {@link Task} that completes
     * when the sub-orchestration completes.
     * <p>
     * See {@link #callSubOrchestrator(String, Object, String, TaskOptions, Class)} for a full description.
     *
     * @see #callSubOrchestrator(String, Object, String, TaskOptions, Class)
     * @param name the name of the orchestrator to invoke
     * @return a new {@link Task} that completes when the sub-orchestration completes or fails
     */
    default Task<Void> callSubOrchestrator(String name){
        return this.callSubOrchestrator(name, null);
    }

    /**
     * Asynchronously invokes another orchestrator as a sub-orchestration and returns a {@link Task} that completes
     * when the sub-orchestration completes.
     * <p>
     * See {@link #callSubOrchestrator(String, Object, String, TaskOptions, Class)} for a full description.
     *
     * @param name the name of the orchestrator to invoke
     * @param input the serializable input to send to the sub-orchestration
     * @return a new {@link Task} that completes when the sub-orchestration completes or fails
     */
    default Task<Void> callSubOrchestrator(String name, Object input) {
        return this.callSubOrchestrator(name, input, null);
    }

    /**
     * Asynchronously invokes another orchestrator as a sub-orchestration and returns a {@link Task} that completes
     * when the sub-orchestration completes.
     * <p>
     * See {@link #callSubOrchestrator(String, Object, String, TaskOptions, Class)} for a full description.
     *
     * @param name the name of the orchestrator to invoke
     * @param input the serializable input to send to the sub-orchestration
     * @param returnType the expected class type of the sub-orchestration output
     * @param <V> the expected type of the sub-orchestration output
     * @return a new {@link Task} that completes when the sub-orchestration completes or fails
     */
    default <V>Task<V> callSubOrchestrator(String name, Object input, Class<V> returnType) {
        return this.callSubOrchestrator(name, input, null, returnType);
    }

    /**
     * Asynchronously invokes another orchestrator as a sub-orchestration and returns a {@link Task} that completes
     * when the sub-orchestration completes.
     * <p>
     * See {@link #callSubOrchestrator(String, Object, String, TaskOptions, Class)} for a full description.
     *
     * @param name the name of the orchestrator to invoke
     * @param input the serializable input to send to the sub-orchestration
     * @param instanceID the unique ID of the sub-orchestration
     * @param returnType the expected class type of the sub-orchestration output
     * @param <V> the expected type of the sub-orchestration output
     * @return a new {@link Task} that completes when the sub-orchestration completes or fails
     */
    default <V> Task<V> callSubOrchestrator(String name, Object input, String instanceID, Class<V> returnType) {
        return this.callSubOrchestrator(name, input, instanceID, null, returnType);
    }

    /**
     * Asynchronously invokes another orchestrator as a sub-orchestration and returns a {@link Task} that completes
     * when the sub-orchestration completes.
     * <p>
     * See {@link #callSubOrchestrator(String, Object, String, TaskOptions, Class)} for a full description.
     *
     * @param name the name of the orchestrator to invoke
     * @param input the serializable input to send to the sub-orchestration
     * @param instanceID the unique ID of the sub-orchestration
     * @param options additional options that control the execution and processing of the activity
     * @return a new {@link Task} that completes when the sub-orchestration completes or fails
     */
    default Task<Void> callSubOrchestrator(String name, Object input, String instanceID, TaskOptions options) {
        return this.callSubOrchestrator(name, input, instanceID, options, Void.class);
    }

    /**
     * Asynchronously invokes another orchestrator as a sub-orchestration and returns a {@link Task} that completes
     * when the sub-orchestration completes. If the sub-orchestration completes successfully, the returned
     * {@code Task}'s value will be the activity's output. If the sub-orchestration fails, the returned {@code Task}
     * will complete exceptionally with a {@link TaskFailedException}.
     * <p>
     * A sub-orchestration has its own instance ID, history, and status that is independent of the parent orchestrator
     * that started it. There are many advantages to breaking down large orchestrations into sub-orchestrations:
     * <ul>
     *     <li>
     *         Splitting large orchestrations into a series of smaller sub-orchestrations can make code more maintainable.
     *     </li>
     *     <li>
     *         Distributing orchestration logic across multiple compute nodes concurrently is useful if
     *         orchestration logic otherwise needs to coordinate a lot of tasks.
     *     </li>
     *     <li>
     *         Memory usage and CPU overhead can be reduced by keeping the history of parent orchestrations smaller.
     *     </li>
     * </ul>
     * The disadvantage is that there is overhead associated with starting a sub-orchestration and processing its
     * output. This is typically only an issue for very small orchestrations.
     * <p>
     * Because sub-orchestrations are independent of their parents, terminating a parent orchestration does not affect
     * any sub-orchestrations. Sub-orchestrations must be terminated independently using their unique instance ID,
     * which is specified using the {@code instanceID} parameter
     *
     * @param name the name of the orchestrator to invoke
     * @param input the serializable input to send to the sub-orchestration
     * @param instanceID the unique ID of the sub-orchestration
     * @param options additional options that control the execution and processing of the activity
     * @param returnType the expected class type of the sub-orchestration output
     * @param <V> the expected type of the sub-orchestration output
     * @return a new {@link Task} that completes when the sub-orchestration completes or fails
     */
    <V> Task<V> callSubOrchestrator(
            String name,
            @Nullable Object input,
            @Nullable String instanceID,
            @Nullable TaskOptions options,
            Class<V> returnType);

    /**
     * Waits for an event to be raised named {@code name} and returns a {@link Task} that completes when the event is
     * received or is canceled when {@code timeout} expires.
     * <p>
     * External clients can raise events to a waiting orchestration instance using the
     * {@link DurableTaskClient#raiseEvent} method.
     * <p>
     * If the current orchestration is not yet waiting for an event named {@code name}, then the event will be saved in
     * the orchestration instance state and dispatched immediately when this method is called. This event saving occurs
     * even if the current orchestrator cancels the wait operation before the event is received.
     * <p>
     * Orchestrators can wait for the same event name multiple times, so waiting for multiple events with the same name
     * is allowed. Each external event received by an orchestrator will complete just one task returned by this method.
     *
     * @param name the case-insensitive name of the event to wait for
     * @param timeout the amount of time to wait before canceling the returned {@code Task}
     * @param dataType the expected class type of the event data payload
     * @param <V> the expected type of the event data payload
     * @return a new {@link Task} that completes when the external event is received or when {@code timeout} expires
     * @throws TaskCanceledException if the specified {@code timeout} value expires before the event is received
     */
    <V> Task<V> waitForExternalEvent(String name, Duration timeout, Class<V> dataType) throws TaskCanceledException;

    /**
     * Waits for an event to be raised named {@code name} and returns a {@link Task} that completes when the event is
     * received or is canceled when {@code timeout} expires.
     * <p>
     * See {@link #waitForExternalEvent(String, Duration, Class)} for a full description.
     *
     * @param name the case-insensitive name of the event to wait for
     * @param timeout the amount of time to wait before canceling the returned {@code Task}
     * @return a new {@link Task} that completes when the external event is received or when {@code timeout} expires
     * @throws TaskCanceledException if the specified {@code timeout} value expires before the event is received
     */
    default Task<Void> waitForExternalEvent(String name, Duration timeout) throws TaskCanceledException {
        return this.waitForExternalEvent(name, timeout, Void.class);
    }

    /**
     * Waits for an event to be raised named {@code name} and returns a {@link Task} that completes when the event is
     * received.
     * <p>
     * See {@link #waitForExternalEvent(String, Duration, Class)} for a full description.
     *
     * @param name the case-insensitive name of the event to wait for
     * @return a new {@link Task} that completes when the external event is received
     */
    default Task<Void> waitForExternalEvent(String name) {
        return this.waitForExternalEvent(name, Void.class);
    }

    /**
     * Waits for an event to be raised named {@code name} and returns a {@link Task} that completes when the event is
     * received.
     * <p>
     * See {@link #waitForExternalEvent(String, Duration, Class)} for a full description.
     *
     * @param name the case-insensitive name of the event to wait for
     * @param dataType the expected class type of the event data payload
     * @param <V> the expected type of the event data payload
     * @return a new {@link Task} that completes when the external event is received
     */
    default <V> Task<V> waitForExternalEvent(String name, Class<V> dataType) {
        try {
            return this.waitForExternalEvent(name, null, dataType);
        } catch (TaskCanceledException e) {
            // This should never happen because of the max duration
            throw new RuntimeException("An unexpected exception was throw while waiting for an external event.", e);
        }
    }

    /**
     * Assigns a custom status value to the current orchestration.
     * <p>
     * The {@code customStatus} value is serialized and stored in orchestration state and will be made available to the
     * orchestration status query APIs, such as {@link DurableTaskClient#getInstanceMetadata}. The serialized value
     * must not exceed 16 KB of UTF-16 encoded text.
     * <p>
     * Use {@link #clearCustomStatus()} to remove the custom status value from the orchestration state.
     *
     * @param customStatus A serializable value to assign as the custom status value.
     */
    void setCustomStatus(Object customStatus);

    /**
     * Clears the orchestration's custom status.
     */
    void clearCustomStatus();
}
