// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.time.Instant;

/**
 * Options for starting a new instance of an orchestration.
 */
public final class NewOrchestrationInstanceOptions {
    private String version;
    private String instanceId;
    private Object input;
    private Instant startTime;
    private String traceParent;
    private String traceState;

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

    public String getTraceParent() {
        return traceParent;
    }

    public void setTraceParent(String traceParent) {
        this.traceParent = traceParent;
    }

    public String getTraceState() {
        return traceState;
    }

    public void setTraceState(String traceState) {
        this.traceState = traceState;
    }
}
