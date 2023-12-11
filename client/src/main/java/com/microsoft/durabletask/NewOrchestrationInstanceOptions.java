// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.microsoft.durabletask.client.InstanceIdReuseAction;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Options for starting a new instance of an orchestration.
 */
public final class NewOrchestrationInstanceOptions {
    private String version;
    private String instanceId;
    private Object input;
    private Instant startTime;
    private final Set<OrchestrationRuntimeStatus> targetStatuses = new HashSet<>();
    private InstanceIdReuseAction instanceIdReuseAction;

    /**
     * Default constructor for the {@link NewOrchestrationInstanceOptions} class.
     */
    public NewOrchestrationInstanceOptions() {
    }

    /**
     * Sets the version of the orchestration to start.
     *
     * @param version the user-defined version of the orchestration
     * @return this {@link NewOrchestrationInstanceOptions} object
     */
    public NewOrchestrationInstanceOptions setVersion(String version) {
        this.version = version;
        return this;
    }

    /**
     * Sets the instance ID of the orchestration to start.
     * <p>
     * If no instance ID is configured, the orchestration will be created with a randomly generated instance ID.
     *
     * @param instanceId the ID of the new orchestration instance
     * @return this {@link NewOrchestrationInstanceOptions} object
     */
    public NewOrchestrationInstanceOptions setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    /**
     * Sets the input of the orchestration to start.
     * <p>
     * There are no restrictions on the type of inputs that can be used except that they must be serializable using
     * the {@link DataConverter} that was configured for the {@link DurableTaskClient} at creation time.
     *
     * @param input the input of the new orchestration instance
     * @return this {@link NewOrchestrationInstanceOptions} object
     */
    public NewOrchestrationInstanceOptions setInput(Object input) {
        this.input = input;
        return this;
    }

    /**
     * Sets the start time of the new orchestration instance.
     * <p>
     * By default, new orchestration instances start executing immediately. This method can be used
     * to start them at a specific time in the future.
     *
     * @param startTime the start time of the new orchestration instance
     * @return this {@link NewOrchestrationInstanceOptions} object
     */
    public NewOrchestrationInstanceOptions setStartTime(Instant startTime) {
        this.startTime = startTime;
        return this;
    }

    /**
     * Sets the target statuses for the reuse orchestration ID policy of the new orchestration instance.
     * This method allows specifying the desired statuses for orchestrations with the same ID
     * when configuring the orchestration ID reuse policy.
     *
     * <p>
     * By default, the {@code targetStatuses} is empty. If an orchestration with the same instance ID
     * already exists, an error will be thrown, indicating a duplicate orchestration instance.
     * You can customize the orchestration ID reuse policy by setting the {@code targetStatuses}
     * and {@code instanceIdReuseAction}.
     *
     * <p>
     * For example, the following options will terminate an existing orchestration instance with the same instance ID
     * if it's in RUNNING, FAILED, or COMPLETED runtime status:
     * <pre>{@code
     * NewOrchestrationInstanceOptions options = new NewOrchestrationInstanceOptions();
     * options.addTargetStatus(OrchestrationRuntimeStatus.RUNNING, OrchestrationRuntimeStatus.FAILED,
     *                         OrchestrationRuntimeStatus.COMPLETED);
     * options.setInstanceIdReuseAction(InstanceIdReuseAction.TERMINATE);
     * }</pre>
     *
     * @param statuses The target statuses for the reuse orchestration ID policy when creating the new orchestration instance.
     * @return This {@link NewOrchestrationInstanceOptions} object.
     */
    public NewOrchestrationInstanceOptions addTargetStatus(OrchestrationRuntimeStatus... statuses) {
        for (OrchestrationRuntimeStatus status : statuses) {
            this.addTargetStatus(status);
        }
        return this;
    }

    /**
     * Sets the target statuses for the reuse orchestration ID policy of the new orchestration instance.
     * This method allows specifying the desired statuses for orchestrations with the same ID
     * when configuring the orchestration ID reuse policy.
     *
     * <p>
     * By default, the {@code targetStatuses} is empty. If an orchestration with the same instance ID
     * already exists, an error will be thrown, indicating a duplicate orchestration instance.
     * You can customize the orchestration ID reuse policy by setting the {@code targetStatuses}
     * and {@code instanceIdReuseAction}.
     *
     * <p>
     * For example, the following options will terminate an existing orchestration instance with the same instance ID
     * if it's in RUNNING, FAILED, or COMPLETED runtime status:
     *<pre>{@code
     * NewOrchestrationInstanceOptions option = new NewOrchestrationInstanceOptions();
     * List<OrchestrationRuntimeStatus> statuses = new ArrayList<>();
     * statuses.add(RUNNING);
     * statuses.add(FAILED);
     * statuses.add(COMPLETED);
     * option.setTargetStatus(statuses);
     * option.setInstanceIdReuseAction(TERMINATE);
     * }
     *</pre>
     * @param statuses A list of target statuses for the reuse orchestration ID policy of creating the new orchestration instance.
     * @return this {@link NewOrchestrationInstanceOptions} object
     */
    public NewOrchestrationInstanceOptions setTargetStatus(List<OrchestrationRuntimeStatus> statuses) {
        for (OrchestrationRuntimeStatus status : statuses) {
            this.addTargetStatus(status);
        }
        return this;
    }

    private void addTargetStatus(OrchestrationRuntimeStatus status) {
        this.targetStatuses.add(status);
    }

    /**
     * Sets the target action for the reuse orchestration ID policy of the new orchestration instance.
     * This method allows specifying the desired action for orchestrations with the same ID
     * when configuring the orchestration ID reuse policy.
     *
     * <p>
     * By default, the {@code instanceIdReuseAction} is {@code InstanceIdReuseAction.ERROR}. If an orchestration with the same instance ID
     * already exists, an error will be thrown, indicating a duplicate orchestration instance.
     * You can customize the orchestration ID reuse policy by setting the {@code targetStatuses}
     * and {@code instanceIdReuseAction}.
     *
     * <p>
     * For example, the following options will terminate an existing orchestration instance with the same instance ID
     * if it's in RUNNING, FAILED, or COMPLETED runtime status:
     * <pre>{@code
     * NewOrchestrationInstanceOptions options = new NewOrchestrationInstanceOptions();
     * options.addTargetStatus(OrchestrationRuntimeStatus.RUNNING, OrchestrationRuntimeStatus.FAILED,
     *                         OrchestrationRuntimeStatus.COMPLETED);
     * options.setInstanceIdReuseAction(InstanceIdReuseAction.TERMINATE);
     * }</pre>
     *
     * @param instanceIdReuseAction The target action for the reuse orchestration ID policy when creating the new orchestration instance.
     * @return This {@link NewOrchestrationInstanceOptions} object.
     */
    public NewOrchestrationInstanceOptions setInstanceIdReuseAction(InstanceIdReuseAction instanceIdReuseAction) {
        this.instanceIdReuseAction = instanceIdReuseAction;
        return this;
    }

    /**
     * Gets the user-specified version of the new orchestration.
     *
     * @return the user-specified version of the new orchestration.
     */
    public String getVersion() {
        return this.version;
    }

    /**
     * Gets the instance ID of the new orchestration.
     *
     * @return the instance ID of the new orchestration.
     */
    public String getInstanceId() {
        return this.instanceId;
    }

    /**
     * Gets the input of the new orchestration.
     *
     * @return the input of the new orchestration.
     */
    public Object getInput() {
        return this.input;
    }

    /**
     * Gets the configured start time of the new orchestration instance.
     *
     * @return the configured start time of the new orchestration instance.
     */
    public Instant getStartTime() {
        return this.startTime;
    }

    /**
     * Gets the target statuses for the reuse orchestration ID policy of the new orchestration instance.
     *
     * @return The target statuses for the reuse orchestration ID policy when creating the new orchestration instance.
     */
    public Set<OrchestrationRuntimeStatus> getTargetStatuses() {
        return this.targetStatuses;
    }

    /**
     * Gets the target action for the reuse orchestration ID policy of the new orchestration instance.
     *
     * @return The target action for the reuse orchestration ID policy when creating the new orchestration instance.
     */
    public InstanceIdReuseAction getInstanceIdReuseAction() {
        return this.instanceIdReuseAction;
    }
}
