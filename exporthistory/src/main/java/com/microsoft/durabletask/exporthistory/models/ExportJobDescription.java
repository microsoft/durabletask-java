// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Describes the current state and progress of an export job.
 */
public final class ExportJobDescription {

    private String jobId;
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

    // Default constructor for Jackson deserialization
    public ExportJobDescription() {
    }

    @Nonnull
    public String getJobId() { return this.jobId; }
    public void setJobId(@Nonnull String jobId) { this.jobId = jobId; }

    @Nonnull
    public ExportJobStatus getStatus() { return this.status; }
    public void setStatus(@Nonnull ExportJobStatus status) { this.status = status; }

    @Nullable
    public Instant getCreatedAt() { return this.createdAt; }
    public void setCreatedAt(@Nullable Instant createdAt) { this.createdAt = createdAt; }

    @Nullable
    public Instant getLastModifiedAt() { return this.lastModifiedAt; }
    public void setLastModifiedAt(@Nullable Instant lastModifiedAt) { this.lastModifiedAt = lastModifiedAt; }

    @Nullable
    public ExportJobConfiguration getConfig() { return this.config; }
    public void setConfig(@Nullable ExportJobConfiguration config) { this.config = config; }

    @Nullable
    public String getOrchestratorInstanceId() { return this.orchestratorInstanceId; }
    public void setOrchestratorInstanceId(@Nullable String orchestratorInstanceId) { this.orchestratorInstanceId = orchestratorInstanceId; }

    public long getScannedInstances() { return this.scannedInstances; }
    public void setScannedInstances(long scannedInstances) { this.scannedInstances = scannedInstances; }

    public long getExportedInstances() { return this.exportedInstances; }
    public void setExportedInstances(long exportedInstances) { this.exportedInstances = exportedInstances; }

    @Nullable
    public String getLastError() { return this.lastError; }
    public void setLastError(@Nullable String lastError) { this.lastError = lastError; }

    @Nullable
    public ExportCheckpoint getCheckpoint() { return this.checkpoint; }
    public void setCheckpoint(@Nullable ExportCheckpoint checkpoint) { this.checkpoint = checkpoint; }

    @Nullable
    public Instant getLastCheckpointTime() { return this.lastCheckpointTime; }
    public void setLastCheckpointTime(@Nullable Instant lastCheckpointTime) { this.lastCheckpointTime = lastCheckpointTime; }
}
