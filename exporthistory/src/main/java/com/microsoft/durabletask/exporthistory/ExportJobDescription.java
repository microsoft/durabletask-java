// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Client-facing description of an export job, projected from the entity {@link ExportJobState}.
 * <p>
 * Mirrors the .NET {@code Microsoft.DurableTask.ExportHistory.ExportJobDescription}.
 */
public final class ExportJobDescription {

    private String jobId = "";
    private ExportJobStatus status;
    private Instant createdAt;
    private Instant lastModifiedAt;
    private ExportJobConfiguration config;
    private String orchestratorInstanceId;
    private long scannedInstances;
    private long exportedInstances;
    private String lastError;
    private ExportCheckpoint checkpoint;
    private Instant lastCheckpointTime;

    /** Creates an empty {@code ExportJobDescription}. */
    public ExportJobDescription() {
    }

    /**
     * Projects an {@link ExportJobState} into a client-facing description.
     *
     * @param jobId the export job ID
     * @param state the entity state
     * @return the projected description
     */
    public static ExportJobDescription fromState(String jobId, ExportJobState state) {
        ExportJobDescription d = new ExportJobDescription();
        d.jobId = jobId;
        d.status = state.getStatus();
        d.createdAt = state.getCreatedAt();
        d.lastModifiedAt = state.getLastModifiedAt();
        d.config = state.getConfig();
        d.orchestratorInstanceId = state.getOrchestratorInstanceId();
        d.scannedInstances = state.getScannedInstances();
        d.exportedInstances = state.getExportedInstances();
        d.lastError = state.getLastError();
        d.checkpoint = state.getCheckpoint();
        d.lastCheckpointTime = state.getLastCheckpointTime();
        return d;
    }

    /** @return the job identifier. */
    public String getJobId() {
        return this.jobId;
    }

    /**
     * Sets the job identifier.
     *
     * @param jobId the job ID
     */
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    /** @return the export job status. */
    public ExportJobStatus getStatus() {
        return this.status;
    }

    /**
     * Sets the export job status.
     *
     * @param status the status
     */
    public void setStatus(ExportJobStatus status) {
        this.status = status;
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

    /** @return the running orchestrator instance ID, or {@code null}. */
    @Nullable
    public String getOrchestratorInstanceId() {
        return this.orchestratorInstanceId;
    }

    /**
     * Sets the running orchestrator instance ID.
     *
     * @param orchestratorInstanceId the orchestrator instance ID
     */
    public void setOrchestratorInstanceId(@Nullable String orchestratorInstanceId) {
        this.orchestratorInstanceId = orchestratorInstanceId;
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

    /** @return the last error message, or {@code null}. */
    @Nullable
    public String getLastError() {
        return this.lastError;
    }

    /**
     * Sets the last error message.
     *
     * @param lastError the error message
     */
    public void setLastError(@Nullable String lastError) {
        this.lastError = lastError;
    }

    /** @return the resume checkpoint, or {@code null}. */
    @Nullable
    public ExportCheckpoint getCheckpoint() {
        return this.checkpoint;
    }

    /**
     * Sets the resume checkpoint.
     *
     * @param checkpoint the checkpoint
     */
    public void setCheckpoint(@Nullable ExportCheckpoint checkpoint) {
        this.checkpoint = checkpoint;
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
}
