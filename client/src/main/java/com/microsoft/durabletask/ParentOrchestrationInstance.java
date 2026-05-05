// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the parent orchestration of a sub-orchestration.
 * This is available via {@link TaskOrchestrationContext#getParent()} when the
 * current orchestration was started as a sub-orchestration.
 */
public final class ParentOrchestrationInstance {
    private final String name;
    private final String instanceId;

    /**
     * Creates a new ParentOrchestrationInstance.
     *
     * @param name       the name of the parent orchestration
     * @param instanceId the instance ID of the parent orchestration
     */
    public ParentOrchestrationInstance(String name, String instanceId) {
        this.name = name;
        this.instanceId = instanceId;
    }

    /**
     * Gets the name of the parent orchestration.
     *
     * @return the parent orchestration name
     */
    @Nullable
    public String getName() {
        return this.name;
    }

    /**
     * Gets the instance ID of the parent orchestration.
     *
     * @return the parent orchestration instance ID
     */
    @Nullable
    public String getInstanceId() {
        return this.instanceId;
    }

    @Override
    public String toString() {
        return String.format("ParentOrchestrationInstance{name='%s', instanceId='%s'}", name, instanceId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParentOrchestrationInstance)) return false;
        ParentOrchestrationInstance that = (ParentOrchestrationInstance) o;
        return Objects.equals(name, that.name) && Objects.equals(instanceId, that.instanceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, instanceId);
    }
}
