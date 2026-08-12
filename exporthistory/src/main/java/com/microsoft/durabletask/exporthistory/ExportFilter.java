// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.OrchestrationRuntimeStatus;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;

/**
 * Filter criteria for selecting orchestration instances to export.
 */
public final class ExportFilter {

    private Instant completedTimeFrom;
    private Instant completedTimeTo;
    private List<OrchestrationRuntimeStatus> runtimeStatus;

    /** Creates an empty {@code ExportFilter} (for deserialization). */
    public ExportFilter() {
    }

    /**
     * Creates an {@code ExportFilter}.
     *
     * @param completedTimeFrom the inclusive completion-time lower bound
     * @param completedTimeTo   the inclusive completion-time upper bound, or {@code null}
     * @param runtimeStatus     the terminal runtime statuses to filter by, or {@code null}
     */
    public ExportFilter(
            Instant completedTimeFrom,
            @Nullable Instant completedTimeTo,
            @Nullable List<OrchestrationRuntimeStatus> runtimeStatus) {
        this.completedTimeFrom = completedTimeFrom;
        this.completedTimeTo = completedTimeTo;
        this.runtimeStatus = runtimeStatus;
    }

    /** @return the inclusive completion-time lower bound. */
    public Instant getCompletedTimeFrom() {
        return this.completedTimeFrom;
    }

    /**
     * Sets the inclusive completion-time lower bound.
     *
     * @param completedTimeFrom the lower bound
     */
    public void setCompletedTimeFrom(Instant completedTimeFrom) {
        this.completedTimeFrom = completedTimeFrom;
    }

    /** @return the inclusive completion-time upper bound, or {@code null}. */
    @Nullable
    public Instant getCompletedTimeTo() {
        return this.completedTimeTo;
    }

    /**
     * Sets the inclusive completion-time upper bound.
     *
     * @param completedTimeTo the upper bound, or {@code null}
     */
    public void setCompletedTimeTo(@Nullable Instant completedTimeTo) {
        this.completedTimeTo = completedTimeTo;
    }

    /** @return the terminal runtime statuses to filter by, or {@code null}. */
    @Nullable
    public List<OrchestrationRuntimeStatus> getRuntimeStatus() {
        return this.runtimeStatus;
    }

    /**
     * Sets the terminal runtime statuses to filter by.
     *
     * @param runtimeStatus the runtime statuses, or {@code null}
     */
    public void setRuntimeStatus(@Nullable List<OrchestrationRuntimeStatus> runtimeStatus) {
        this.runtimeStatus = runtimeStatus;
    }
}
