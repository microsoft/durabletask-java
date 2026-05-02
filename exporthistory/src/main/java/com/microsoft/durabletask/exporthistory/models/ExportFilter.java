// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import com.microsoft.durabletask.OrchestrationRuntimeStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collection;

/**
 * Filter for querying terminal orchestration instances during export.
 */
public final class ExportFilter {

    private Instant completedTimeFrom;
    private Instant completedTimeTo;
    private Collection<OrchestrationRuntimeStatus> runtimeStatus;

    public ExportFilter() {
    }

    public ExportFilter(
            @Nonnull Instant completedTimeFrom,
            @Nullable Instant completedTimeTo,
            @Nullable Collection<OrchestrationRuntimeStatus> runtimeStatus) {
        if (completedTimeFrom == null) {
            throw new IllegalArgumentException("completedTimeFrom must not be null.");
        }
        this.completedTimeFrom = completedTimeFrom;
        this.completedTimeTo = completedTimeTo;
        this.runtimeStatus = runtimeStatus;
    }

    @Nonnull
    public Instant getCompletedTimeFrom() {
        return this.completedTimeFrom;
    }

    @Nullable
    public Instant getCompletedTimeTo() {
        return this.completedTimeTo;
    }

    @Nullable
    public Collection<OrchestrationRuntimeStatus> getRuntimeStatus() {
        return this.runtimeStatus;
    }
}
