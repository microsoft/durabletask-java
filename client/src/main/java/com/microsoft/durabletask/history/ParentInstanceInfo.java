// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import javax.annotation.Nullable;

/**
 * Information about the parent orchestration that created a sub-orchestration.
 */
public final class ParentInstanceInfo {
    private final int taskScheduledId;
    private final String name;
    private final String version;
    private final OrchestrationInstance orchestrationInstance;

    /**
     * Creates a new {@code ParentInstanceInfo}.
     *
     * @param taskScheduledId       the task scheduled ID of the sub-orchestration in the parent's history
     * @param name                  the parent orchestrator name, or {@code null}
     * @param version               the parent orchestrator version, or {@code null}
     * @param orchestrationInstance the parent orchestration instance, or {@code null}
     */
    public ParentInstanceInfo(
            int taskScheduledId,
            @Nullable String name,
            @Nullable String version,
            @Nullable OrchestrationInstance orchestrationInstance) {
        this.taskScheduledId = taskScheduledId;
        this.name = name;
        this.version = version;
        this.orchestrationInstance = orchestrationInstance;
    }

    /** @return the task scheduled ID of the sub-orchestration in the parent's history. */
    public int getTaskScheduledId() {
        return this.taskScheduledId;
    }

    /** @return the parent orchestrator name, or {@code null} if not set. */
    @Nullable
    public String getName() {
        return this.name;
    }

    /** @return the parent orchestrator version, or {@code null} if not set. */
    @Nullable
    public String getVersion() {
        return this.version;
    }

    /** @return the parent orchestration instance, or {@code null} if not set. */
    @Nullable
    public OrchestrationInstance getOrchestrationInstance() {
        return this.orchestrationInstance;
    }
}
