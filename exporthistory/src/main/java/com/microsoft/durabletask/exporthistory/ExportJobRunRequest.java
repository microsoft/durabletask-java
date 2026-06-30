// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.EntityInstanceId;

/**
 * Input to the {@link ExportJobOrchestrator} identifying the job entity and the number of processed cycles
 * (used to bound work before {@code continueAsNew}).
 * <p>
 * Mirrors the .NET {@code ExportJobRunRequest} record.
 */
public final class ExportJobRunRequest {

    private EntityInstanceId jobEntityId;
    private int processedCycles;

    /** Creates an empty {@code ExportJobRunRequest} (for deserialization). */
    public ExportJobRunRequest() {
    }

    /**
     * Creates an {@code ExportJobRunRequest}.
     *
     * @param jobEntityId     the export job entity ID
     * @param processedCycles the number of cycles already processed in this orchestration generation
     */
    public ExportJobRunRequest(EntityInstanceId jobEntityId, int processedCycles) {
        this.jobEntityId = jobEntityId;
        this.processedCycles = processedCycles;
    }

    /** @return the export job entity ID. */
    public EntityInstanceId getJobEntityId() {
        return this.jobEntityId;
    }

    /**
     * Sets the export job entity ID.
     *
     * @param jobEntityId the entity ID
     */
    public void setJobEntityId(EntityInstanceId jobEntityId) {
        this.jobEntityId = jobEntityId;
    }

    /** @return the number of cycles already processed in this orchestration generation. */
    public int getProcessedCycles() {
        return this.processedCycles;
    }

    /**
     * Sets the number of cycles already processed.
     *
     * @param processedCycles the processed cycle count
     */
    public void setProcessedCycles(int processedCycles) {
        this.processedCycles = processedCycles;
    }
}
