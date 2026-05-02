// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Request to commit a checkpoint with progress updates to the export job entity.
 */
public final class CommitCheckpointRequest {

    private long scannedInstances;
    private long exportedInstances;
    private ExportCheckpoint checkpoint;
    private List<ExportFailure> failures;

    // Default constructor for Jackson deserialization
    public CommitCheckpointRequest() {
    }

    public CommitCheckpointRequest(
            long scannedInstances,
            long exportedInstances,
            @Nullable ExportCheckpoint checkpoint,
            @Nullable List<ExportFailure> failures) {
        this.scannedInstances = scannedInstances;
        this.exportedInstances = exportedInstances;
        this.checkpoint = checkpoint;
        this.failures = failures;
    }

    public long getScannedInstances() { return this.scannedInstances; }
    public void setScannedInstances(long scannedInstances) { this.scannedInstances = scannedInstances; }

    public long getExportedInstances() { return this.exportedInstances; }
    public void setExportedInstances(long exportedInstances) { this.exportedInstances = exportedInstances; }

    @Nullable
    public ExportCheckpoint getCheckpoint() { return this.checkpoint; }
    public void setCheckpoint(@Nullable ExportCheckpoint checkpoint) { this.checkpoint = checkpoint; }

    @Nullable
    public List<ExportFailure> getFailures() { return this.failures; }
    public void setFailures(@Nullable List<ExportFailure> failures) { this.failures = failures; }
}
