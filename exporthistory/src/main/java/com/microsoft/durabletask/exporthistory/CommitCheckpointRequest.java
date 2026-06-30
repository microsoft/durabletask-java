// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Request to commit a checkpoint with progress updates and optional failures.
 * <p>
 * Mirrors the .NET {@code Microsoft.DurableTask.ExportHistory.CommitCheckpointRequest}. When
 * {@link #getCheckpoint()} is non-null the cursor moves forward (successful batch); when {@code null} the cursor is
 * retained (failed batch eligible for retry).
 */
public final class CommitCheckpointRequest {

    private long scannedInstances;
    private long exportedInstances;
    private ExportCheckpoint checkpoint;
    private List<ExportFailure> failures;

    /** Creates an empty {@code CommitCheckpointRequest}. */
    public CommitCheckpointRequest() {
    }

    /** @return the number of instances scanned in this batch. */
    public long getScannedInstances() {
        return this.scannedInstances;
    }

    /**
     * Sets the number of instances scanned in this batch.
     *
     * @param scannedInstances the scanned count
     */
    public void setScannedInstances(long scannedInstances) {
        this.scannedInstances = scannedInstances;
    }

    /** @return the number of instances successfully exported in this batch. */
    public long getExportedInstances() {
        return this.exportedInstances;
    }

    /**
     * Sets the number of instances successfully exported in this batch.
     *
     * @param exportedInstances the exported count
     */
    public void setExportedInstances(long exportedInstances) {
        this.exportedInstances = exportedInstances;
    }

    /** @return the checkpoint to commit, or {@code null} to keep the current checkpoint. */
    @Nullable
    public ExportCheckpoint getCheckpoint() {
        return this.checkpoint;
    }

    /**
     * Sets the checkpoint to commit. If {@code null}, the cursor does not move forward (retry of the same batch).
     *
     * @param checkpoint the checkpoint, or {@code null}
     */
    public void setCheckpoint(@Nullable ExportCheckpoint checkpoint) {
        this.checkpoint = checkpoint;
    }

    /** @return the list of failed instance exports, or {@code null}. */
    @Nullable
    public List<ExportFailure> getFailures() {
        return this.failures;
    }

    /**
     * Sets the list of failed instance exports.
     *
     * @param failures the failures, or {@code null}
     */
    public void setFailures(@Nullable List<ExportFailure> failures) {
        this.failures = failures;
    }
}
