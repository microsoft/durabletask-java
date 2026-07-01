// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Export job state stored in the {@link ExportJob} entity.
 */
public final class ExportJobState {

    private ExportJobStatus status;
    private ExportJobConfiguration config;
    private ExportCheckpoint checkpoint;
    private Instant createdAt;
    private Instant lastModifiedAt;
    private Instant lastCheckpointTime;
    private String lastError;
    private long scannedInstances;
    private long exportedInstances;
    private String orchestratorInstanceId;

    /** Creates an empty {@code ExportJobState}. */
    public ExportJobState() {
    }

    /** @return the current status of the export job. */
    public ExportJobStatus getStatus() {
        return this.status;
    }

    /**
     * Sets the current status of the export job.
     *
     * @param status the status
     */
    public void setStatus(ExportJobStatus status) {
        this.status = status;
    }

    /** @return the export job configuration, or {@code null}. */
    @Nullable
    public ExportJobConfiguration getConfig() {
        return this.config;
    }

    /**
     * Sets the export job configuration.
     *
     * @param config the configuration
     */
    public void setConfig(@Nullable ExportJobConfiguration config) {
        this.config = config;
    }

    /** @return the resume checkpoint, or {@code null}. */
    @Nullable
    public ExportCheckpoint getCheckpoint() {
        return this.checkpoint;
    }

    /**
     * Sets the resume checkpoint.
     *
     * @param checkpoint the checkpoint, or {@code null}
     */
    public void setCheckpoint(@Nullable ExportCheckpoint checkpoint) {
        this.checkpoint = checkpoint;
    }

    /** @return the creation time, or {@code null}. */
    @Nullable
    public Instant getCreatedAt() {
        return this.createdAt;
    }

    /**
     * Sets the creation time.
     *
     * @param createdAt the creation time
     */
    public void setCreatedAt(@Nullable Instant createdAt) {
        this.createdAt = createdAt;
    }

    /** @return the last-modified time, or {@code null}. */
    @Nullable
    public Instant getLastModifiedAt() {
        return this.lastModifiedAt;
    }

    /**
     * Sets the last-modified time.
     *
     * @param lastModifiedAt the last-modified time
     */
    public void setLastModifiedAt(@Nullable Instant lastModifiedAt) {
        this.lastModifiedAt = lastModifiedAt;
    }

    /** @return the time of the last checkpoint, or {@code null}. */
    @Nullable
    public Instant getLastCheckpointTime() {
        return this.lastCheckpointTime;
    }

    /**
     * Sets the time of the last checkpoint.
     *
     * @param lastCheckpointTime the last checkpoint time
     */
    public void setLastCheckpointTime(@Nullable Instant lastCheckpointTime) {
        this.lastCheckpointTime = lastCheckpointTime;
    }

    /** @return the last error message, or {@code null}. */
    @Nullable
    public String getLastError() {
        return this.lastError;
    }

    /**
     * Sets the last error message.
     *
     * @param lastError the error message, or {@code null}
     */
    public void setLastError(@Nullable String lastError) {
        this.lastError = lastError;
    }

    /** @return the total number of instances scanned. */
    public long getScannedInstances() {
        return this.scannedInstances;
    }

    /**
     * Sets the total number of instances scanned.
     *
     * @param scannedInstances the scanned count
     */
    public void setScannedInstances(long scannedInstances) {
        this.scannedInstances = scannedInstances;
    }

    /** @return the total number of instances exported. */
    public long getExportedInstances() {
        return this.exportedInstances;
    }

    /**
     * Sets the total number of instances exported.
     *
     * @param exportedInstances the exported count
     */
    public void setExportedInstances(long exportedInstances) {
        this.exportedInstances = exportedInstances;
    }

    /** @return the instance ID of the orchestrator running this job, or {@code null}. */
    @Nullable
    public String getOrchestratorInstanceId() {
        return this.orchestratorInstanceId;
    }

    /**
     * Sets the instance ID of the orchestrator running this job.
     *
     * @param orchestratorInstanceId the orchestrator instance ID, or {@code null}
     */
    public void setOrchestratorInstanceId(@Nullable String orchestratorInstanceId) {
        this.orchestratorInstanceId = orchestratorInstanceId;
    }
}
