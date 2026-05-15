// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import com.microsoft.durabletask.EntityInstanceId;

import javax.annotation.Nonnull;

/**
 * Input for the {@code ExportJobOrchestrator}.
 */
public final class ExportJobRunRequest {

    private EntityInstanceId jobEntityId;
    private int processedCycles;

    public ExportJobRunRequest(@Nonnull EntityInstanceId jobEntityId) {
        this(jobEntityId, 0);
    }

    public ExportJobRunRequest(@Nonnull EntityInstanceId jobEntityId, int processedCycles) {
        this.jobEntityId = jobEntityId;
        this.processedCycles = processedCycles;
    }

    // Default constructor for Jackson deserialization
    public ExportJobRunRequest() {
        this.jobEntityId = null;
        this.processedCycles = 0;
    }

    @Nonnull
    public EntityInstanceId getJobEntityId() { return this.jobEntityId; }

    public int getProcessedCycles() { return this.processedCycles; }

    public void setJobEntityId(EntityInstanceId jobEntityId) { this.jobEntityId = jobEntityId; }
    public void setProcessedCycles(int processedCycles) { this.processedCycles = processedCycles; }
}
