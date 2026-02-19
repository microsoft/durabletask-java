// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Base class that defines client operations for managing orchestration instances.
 * <p>
 * Instances of this class can be used to start, query, raise events to, and terminate orchestration instances. In most
 * cases, methods on this class accept an instance ID as a parameter, which identifies the orchestration instance.
 * <p>
 * At the time of writing, the most common implementation of this class is <code>DurableTaskGrpcClient</code>,
 * which works by making gRPC calls to a remote service (e.g. a sidecar) that implements the operation behavior. To
 * ensure any owned network resources are properly released, instances of this class should be closed when they are no
 * longer needed.
 * <p>
 * Instances of this class are expected to be safe for multithreaded apps. You can therefore safely cache instances
 * of this class and reuse them across multiple contexts. Caching these objects is useful to improve overall
 * performance.
 */
public abstract class DurableTaskClient implements AutoCloseable {

    /**
     * Releases any network resources held by this object.
     */
    @Override
    public void close() {
        // no default implementation
    }

    /**
     * Schedules a new orchestration instance with a random ID for execution.
     *
     * @param orchestratorName the name of the orchestrator to schedule
     * @return the randomly-generated instance ID of the scheduled orchestration instance
     */
    public String scheduleNewOrchestrationInstance(String orchestratorName) {
        return this.scheduleNewOrchestrationInstance(orchestratorName, null, null);
    }

    /**
     * Schedules a new orchestration instance with a specified input and a random ID for execution.
     *
     * @param orchestratorName the name of the orchestrator to schedule
     * @param input the input to pass to the scheduled orchestration instance. Must be serializable.
     * @return the randomly-generated instance ID of the scheduled orchestration instance
     */
    public String scheduleNewOrchestrationInstance(String orchestratorName, Object input) {
        return this.scheduleNewOrchestrationInstance(orchestratorName, input, null);
    }

    /**
     * Schedules a new orchestration instance with a specified input and ID for execution.
     *
     * @param orchestratorName the name of the orchestrator to schedule
     * @param input the input to pass to the scheduled orchestration instance. Must be serializable.
     * @param instanceId the unique ID of the orchestration instance to schedule
     * @return the <code>instanceId</code> parameter value
     */
    public String scheduleNewOrchestrationInstance(String orchestratorName, Object input, String instanceId) {
        NewOrchestrationInstanceOptions options = new NewOrchestrationInstanceOptions()
                .setInput(input)
                .setInstanceId(instanceId);
        return this.scheduleNewOrchestrationInstance(orchestratorName, options);
    }

    /**
     * Schedules a new orchestration instance with a specified set of options for execution.
     *
     * @param orchestratorName the name of the orchestrator to schedule
     * @param options the options for the new orchestration instance, including input, instance ID, etc.
     * @return the ID of the scheduled orchestration instance, which was either provided in <code>options</code>
     *         or randomly generated
     */
    public abstract String scheduleNewOrchestrationInstance(
            String orchestratorName,
            NewOrchestrationInstanceOptions options);

    /**
     * Sends an event notification message to a waiting orchestration instance.
     * <p>
     * In order to handle the event, the target orchestration instance must be waiting for an event named
     * <code>eventName</code> using the {@link TaskOrchestrationContext#waitForExternalEvent(String)} method.
     * If the target orchestration instance is not yet waiting for an event named <code>eventName</code>,
     * then the event will be saved in the orchestration instance state and dispatched immediately when the
     * orchestrator calls {@link TaskOrchestrationContext#waitForExternalEvent(String)}. This event saving occurs even
     * if the orchestrator has canceled its wait operation before the event was received.
     * <p>
     * Raised events for a completed or non-existent orchestration instance will be silently discarded.
     *
     * @param instanceId the ID of the orchestration instance that will handle the event
     * @param eventName the case-insensitive name of the event
     */
    public void raiseEvent(String instanceId, String eventName) {
        this.raiseEvent(instanceId, eventName, null);
    }

    /**
     * Sends an event notification message with a payload to a waiting orchestration instance.
     * <p>
     * In order to handle the event, the target orchestration instance must be waiting for an event named
     * <code>eventName</code> using the {@link TaskOrchestrationContext#waitForExternalEvent(String)} method.
     * If the target orchestration instance is not yet waiting for an event named <code>eventName</code>,
     * then the event will be saved in the orchestration instance state and dispatched immediately when the
     * orchestrator calls {@link TaskOrchestrationContext#waitForExternalEvent(String)}. This event saving occurs even
     * if the orchestrator has canceled its wait operation before the event was received.
     * <p>
     * Raised events for a completed or non-existent orchestration instance will be silently discarded.
     *
     * @param instanceId the ID of the orchestration instance that will handle the event
     * @param eventName the case-insensitive name of the event
     * @param eventPayload the serializable data payload to include with the event
     */
    public abstract void raiseEvent(String instanceId, String eventName, @Nullable Object eventPayload);

    /**
     * Fetches orchestration instance metadata from the configured durable store.
     *
     * @param instanceId the unique ID of the orchestration instance to fetch
     * @param getInputsAndOutputs <code>true</code> to fetch the orchestration instance's inputs, outputs, and custom
     *                            status, or <code>false</code> to omit them
     * @return a metadata record that describes the orchestration instance and its execution status, or
     *         a default instance if no such instance is found. Please refer to method
     *         {@link OrchestrationMetadata#isInstanceFound()} to check if an instance is found.
     */
    @Nullable
    public abstract OrchestrationMetadata getInstanceMetadata(String instanceId, boolean getInputsAndOutputs);

    /**
     * Waits for an orchestration to start running and returns an {@link OrchestrationMetadata} object that contains
     * metadata about the started instance.
     * <p>
     * A "started" orchestration instance is any instance not in the <code>Pending</code> state.
     * <p>
     * If an orchestration instance is already running when this method is called, the method will return immediately.
     * <p>
     * Note that this method overload will not fetch the orchestration's inputs, outputs, or custom status payloads.
     *
     * @param instanceId the unique ID of the orchestration instance to wait for
     * @param timeout the amount of time to wait for the orchestration instance to start
     * @throws TimeoutException when the orchestration instance is not started within the specified amount of time
     * @return the orchestration instance metadata or <code>null</code> if no such instance is found
     */
    @Nullable
    public OrchestrationMetadata waitForInstanceStart(String instanceId, Duration timeout) throws TimeoutException {
        return this.waitForInstanceStart(instanceId, timeout, false);
    }

    /**
     * Waits for an orchestration to start running and returns an {@link OrchestrationMetadata} object that contains
     * metadata about the started instance and optionally its input, output, and custom status payloads.
     * <p>
     * A "started" orchestration instance is any instance not in the <code>Pending</code> state.
     * <p>
     * If an orchestration instance is already running when this method is called, the method will return immediately.
     *
     * @param instanceId the unique ID of the orchestration instance to wait for
     * @param timeout the amount of time to wait for the orchestration instance to start
     * @param getInputsAndOutputs <code>true</code> to fetch the orchestration instance's inputs, outputs, and custom
     *                            status, or <code>false</code> to omit them
     * @throws TimeoutException when the orchestration instance is not started within the specified amount of time
     * @return the orchestration instance metadata or <code>null</code> if no such instance is found
     */
    @Nullable
    public abstract OrchestrationMetadata waitForInstanceStart(
            String instanceId,
            Duration timeout,
            boolean getInputsAndOutputs) throws TimeoutException;

    /**
     * Waits for an orchestration to complete and returns an {@link OrchestrationMetadata} object that contains
     * metadata about the completed instance.
     * <p>
     * A "completed" orchestration instance is any instance in one of the terminal states. For example, the
     * <code>Completed</code>, <code>Failed</code>, or <code>Terminated</code> states.
     * <p>
     * Orchestrations are long-running and could take hours, days, or months before completing.
     * Orchestrations can also be eternal, in which case they'll never complete unless terminated.
     * In such cases, this call may block indefinitely, so care must be taken to ensure appropriate timeouts are used.
     * <p>
     * If an orchestration instance is already complete when this method is called, the method will return immediately.
     *
     * @param instanceId the unique ID of the orchestration instance to wait for
     * @param timeout the amount of time to wait for the orchestration instance to complete
     * @param getInputsAndOutputs <code>true</code> to fetch the orchestration instance's inputs, outputs, and custom
     *                            status, or <code>false</code> to omit them
     * @throws TimeoutException when the orchestration instance is not completed within the specified amount of time
     * @return the orchestration instance metadata or <code>null</code> if no such instance is found
     */
    @Nullable
    public abstract OrchestrationMetadata waitForInstanceCompletion(
            String instanceId,
            Duration timeout,
            boolean getInputsAndOutputs) throws TimeoutException;

    /**
     * Terminates a running orchestration instance and updates its runtime status to <code>Terminated</code>.
     * <p>
     * This method internally enqueues a "terminate" message in the task hub. When the task hub worker processes
     * this message, it will update the runtime status of the target instance to <code>Terminated</code>.
     * You can use the {@link #waitForInstanceCompletion} to wait for the instance to reach the terminated state.
     * <p>
     * Terminating an orchestration instance has no effect on any in-flight activity function executions
     * or sub-orchestrations that were started by the terminated instance. Those actions will continue to run
     * without interruption. However, their results will be discarded. If you want to terminate sub-orchestrations,
     * you must issue separate terminate commands for each sub-orchestration instance.
     * <p>
     * At the time of writing, there is no way to terminate an in-flight activity execution.
     * <p>
     * Attempting to terminate a completed or non-existent orchestration instance will fail silently.
     *
     * @param instanceId the unique ID of the orchestration instance to terminate
     * @param output the optional output to set for the terminated orchestration instance.
     *               This value must be serializable.
     */
    public abstract void terminate(String instanceId, @Nullable Object output);

    /**
     * Fetches orchestration instance metadata from the configured durable store using a status query filter.
     *
     * @param query filter criteria that determines which orchestrations to fetch data for.
     * @return the result of the query operation, including instance metadata and possibly a continuation token
     */
    public abstract OrchestrationStatusQueryResult queryInstances(OrchestrationStatusQuery query);

    /**
     * Initializes the target task hub data store.
     * <p>
     * This is an administrative operation that only needs to be done once for the lifetime of the task hub.
     *
     * @param recreateIfExists <code>true</code> to delete any existing task hub first; <code>false</code> to make this
     *                         operation a no-op if the task hub data store already exists. Note that deleting a task
     *                         hub will result in permanent data loss. Use this operation with care.
     */
    public abstract void createTaskHub(boolean recreateIfExists);

    /**
     * Permanently deletes the target task hub data store and any orchestration data it may contain.
     * <p>
     * This is an administrative operation that is irreversible. It should be used with great care.
     */
    public abstract void deleteTaskHub();

    /**
     * Purges orchestration instance metadata from the durable store.
     * <p>
     * This method can be used to permanently delete orchestration metadata from the underlying storage provider,
     * including any stored inputs, outputs, and orchestration history records. This is often useful for implementing
     * data retention policies and for keeping storage costs minimal. Only orchestration instances in the
     * <code>Completed</code>, <code>Failed</code>, or <code>Terminated</code> state can be purged.
     * <p>
     * If the target orchestration instance is not found in the data store, or if the instance is found but not in a
     * terminal state, then the returned {@link PurgeResult} will report that zero instances were purged.
     * Otherwise, the existing data will be purged and the returned {@link PurgeResult} will report that one instance
     * was purged.
     *
     * @param instanceId the unique ID of the orchestration instance to purge
     * @return the result of the purge operation, including the number of purged orchestration instances (0 or 1)
     */
    public abstract PurgeResult purgeInstance(String instanceId);

    /**
     * Purges orchestration instance metadata from the durable store using a filter that determines which instances to
     * purge data for.
     * <p>
     * This method can be used to permanently delete orchestration metadata from the underlying storage provider,
     * including any stored inputs, outputs, and orchestration history records. This is often useful for implementing
     * data retention policies and for keeping storage costs minimal. Only orchestration instances in the
     * <code>Completed</code>, <code>Failed</code>, or <code>Terminated</code> state can be purged.
     * <p>
     * Depending on the type of the durable store, purge operations that target multiple orchestration instances may
     * take a long time to complete and be resource intensive. It may therefore be useful to break up purge operations
     * into multiple method calls over a period of time and have them cover smaller time windows.
     *
     * @param purgeInstanceCriteria orchestration instance filter criteria used to determine which instances to purge
     * @throws TimeoutException when purging instances is not completed within the specified amount of time.
     *         The default timeout for purging instances is 10 minutes
     * @return the result of the purge operation, including the number of purged orchestration instances (0 or 1)
     */
    public abstract PurgeResult purgeInstances(PurgeInstanceCriteria purgeInstanceCriteria) throws TimeoutException;

    /**
     * Restarts an existing orchestration instance with the original input.
     * @param instanceId the ID of the previously run orchestration instance to restart.
     * @param restartWithNewInstanceId <code>true</code> to restart the orchestration instance with a new instance ID
     *                                 <code>false</code> to restart the orchestration instance with same instance ID
     * @return the ID of the scheduled orchestration instance, which is either <code>instanceId</code> or randomly
     *         generated depending on the value of <code>restartWithNewInstanceId</code>
     */
    public abstract String restartInstance(String instanceId, boolean restartWithNewInstanceId);

    /**
     * Rewinds a failed orchestration instance to the last known good state and replays from there.
     * <p>
     * This method can only be used on orchestration instances that are in a <code>Failed</code> state.
     * When rewound, the orchestration instance will restart from the point of failure as if the failure
     * never occurred. It rewinds the orchestration by replaying any
     * Failed Activities and Failed suborchestrations that themselves have Failed Activities
     *
     * @param instanceId the ID of the orchestration instance to rewind
     */
    public void rewindInstance(String instanceId) {
        this.rewindInstance(instanceId, null);
    }

    /**
     * Rewinds a failed orchestration instance to the last known good state and replays from there.
     * <p>
     * This method can only be used on orchestration instances that are in a <code>Failed</code> state.
     * When rewound, the orchestration instance will restart from the point of failure as if the failure
     * never occurred. It rewinds the orchestration by replaying any
     * Failed Activities and Failed suborchestrations that themselves have Failed Activities
     *
     * @param instanceId the ID of the orchestration instance to rewind
     * @param reason the reason for rewinding the orchestration instance
     */
    public abstract void rewindInstance(String instanceId, @Nullable String reason);

    /**
     * Suspends a running orchestration instance.
     * @param instanceId the ID of the orchestration instance to suspend
     */
    public void suspendInstance (String instanceId) {
        this.suspendInstance(instanceId, null);
    }

    /**
     * Resumes a running orchestration instance.
     * @param instanceId the ID of the orchestration instance to resume
     */
    public void resumeInstance(String instanceId) {
        this.resumeInstance(instanceId, null);
    }

    /**
     * Suspends a running orchestration instance.
     * @param instanceId the ID of the orchestration instance to suspend
     * @param reason the reason for suspending the orchestration instance
     */
    public abstract void suspendInstance(String instanceId, @Nullable String reason);

    /**
     * Resumes a running orchestration instance.
     * @param instanceId the ID of the orchestration instance to resume
     * @param reason the reason for resuming the orchestration instance
     */
    public abstract void resumeInstance(String instanceId, @Nullable String reason);
}