// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;

/**
 * Identifies an orchestration instance and, optionally, a specific execution (generation) of it.
 */
public final class OrchestrationInstance {
    private final String instanceId;
    private final String executionId;

    /**
     * Creates a new {@code OrchestrationInstance}.
     *
     * @param instanceId  the orchestration instance ID
     * @param executionId the execution (generation) ID, or {@code null}
     */
    public OrchestrationInstance(String instanceId, @Nullable String executionId) {
        this.instanceId = instanceId;
        this.executionId = executionId;
    }

    /** @return the unique ID of the orchestration instance. */
    public String getInstanceId() {
        return this.instanceId;
    }

    /** @return the execution (generation) ID, or {@code null} if not set. */
    @Nullable
    public String getExecutionId() {
        return this.executionId;
    }
}
