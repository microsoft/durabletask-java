// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import com.microsoft.durabletask.OrchestrationRuntimeStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collection;

/**
 * Input for the {@code ListTerminalInstancesActivity}.
 */
public final class ListTerminalInstancesRequest {

    private Instant completedTimeFrom;
    private Instant completedTimeTo;
    private Collection<OrchestrationRuntimeStatus> runtimeStatus;
    private String lastInstanceKey;
    private int maxInstancesPerBatch;

    public ListTerminalInstancesRequest(
            @Nonnull Instant completedTimeFrom,
            @Nullable Instant completedTimeTo,
            @Nullable Collection<OrchestrationRuntimeStatus> runtimeStatus,
            @Nullable String lastInstanceKey,
            int maxInstancesPerBatch) {
        this.completedTimeFrom = completedTimeFrom;
        this.completedTimeTo = completedTimeTo;
        this.runtimeStatus = runtimeStatus;
        this.lastInstanceKey = lastInstanceKey;
        this.maxInstancesPerBatch = maxInstancesPerBatch;
    }

    // Default constructor for Jackson deserialization
    public ListTerminalInstancesRequest() {
        this.completedTimeFrom = null;
        this.completedTimeTo = null;
        this.runtimeStatus = null;
        this.lastInstanceKey = null;
        this.maxInstancesPerBatch = 100;
    }

    @Nonnull
    public Instant getCompletedTimeFrom() { return this.completedTimeFrom; }

    @Nullable
    public Instant getCompletedTimeTo() { return this.completedTimeTo; }

    @Nullable
    public Collection<OrchestrationRuntimeStatus> getRuntimeStatus() { return this.runtimeStatus; }

    @Nullable
    public String getLastInstanceKey() { return this.lastInstanceKey; }

    public int getMaxInstancesPerBatch() { return this.maxInstancesPerBatch; }

    public void setCompletedTimeFrom(Instant completedTimeFrom) { this.completedTimeFrom = completedTimeFrom; }
    public void setCompletedTimeTo(Instant completedTimeTo) { this.completedTimeTo = completedTimeTo; }
    public void setRuntimeStatus(Collection<OrchestrationRuntimeStatus> runtimeStatus) { this.runtimeStatus = runtimeStatus; }
    public void setLastInstanceKey(String lastInstanceKey) { this.lastInstanceKey = lastInstanceKey; }
    public void setMaxInstancesPerBatch(int maxInstancesPerBatch) { this.maxInstancesPerBatch = maxInstancesPerBatch; }
}
