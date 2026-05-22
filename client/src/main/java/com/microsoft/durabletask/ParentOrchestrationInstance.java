// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Represents the parent orchestration of a sub-orchestration.
 * This is available via {@link TaskOrchestrationContext#getParentInstance()} when the
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
    public ParentOrchestrationInstance(@Nonnull String name, @Nonnull String instanceId) {
        this.name = Objects.requireNonNull(name, "name");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
    }

    /**
     * Gets the name of the parent orchestration.
     *
     * @return the parent orchestration name
     */
    @Nonnull
    public String getName() {
        return this.name;
    }

    /**
     * Gets the instance ID of the parent orchestration.
     *
     * @return the parent orchestration instance ID
     */
    @Nonnull
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
