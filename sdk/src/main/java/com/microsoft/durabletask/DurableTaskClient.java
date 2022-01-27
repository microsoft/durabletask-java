// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.time.Duration;

// TODO: JavaDocs
public abstract class DurableTaskClient implements AutoCloseable {
    public void close() {
        // no default implementation
    }

    public String scheduleNewOrchestrationInstance(String orchestratorName) {
        return this.scheduleNewOrchestrationInstance(orchestratorName, null, null);
    }

    public String scheduleNewOrchestrationInstance(String orchestratorName, Object input) {
        return this.scheduleNewOrchestrationInstance(orchestratorName, input, null);
    }

    public String scheduleNewOrchestrationInstance(String orchestratorName, Object input, String instanceId) {
        NewOrchestrationInstanceOptions options = NewOrchestrationInstanceOptions.newBuilder()
                .setInput(input)
                .setInstanceId(instanceId)
                .build();
        return this.scheduleNewOrchestrationInstance(orchestratorName, options);
    }

    public abstract String scheduleNewOrchestrationInstance(
            String orchestratorName,
            NewOrchestrationInstanceOptions options);

    public void raiseEvent(String instanceId, String eventName) {
        this.raiseEvent(instanceId, eventName, null);
    }

    /**
     * Sends an event notification message to a waiting orchestration instance.
     * </p>
     * In order to handle the event, the target orchestration instance must be waiting for an
     * event named <a href="#{@link}">{@link eventName}</a> using the WaitForExternalEvent API.
     * 
     * <p>TODO: Remaining documentation
     * 
     * @param instanceId The ID of the orchestration instance that will handle the event.
     * @param eventName The name of the event. Event names are case-insensitive.
     * @param eventPayload The serializable data payload to include with the event.
     */
    public abstract void raiseEvent(String instanceId, String eventName, Object eventPayload);

    public abstract OrchestrationMetadata getInstanceMetadata(String instanceId, boolean getInputsAndOutputs);

    public OrchestrationMetadata waitForInstanceStart(String instanceId, Duration timeout) {
        return this.waitForInstanceStart(instanceId, timeout, false);
    }

    public abstract OrchestrationMetadata waitForInstanceStart(String instanceId, Duration timeout, boolean getInputsAndOutputs);

    public OrchestrationMetadata waitForInstanceCompletion(String instanceId, Duration timeout) {
        return this.waitForInstanceCompletion(instanceId, timeout, false);
    }

    public abstract OrchestrationMetadata waitForInstanceCompletion(String instanceId, Duration timeout, boolean getInputsAndOutputs);
}