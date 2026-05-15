// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Mutable state of an export job, persisted as entity state.
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

    // Default constructor for Jackson deserialization
    public ExportJobState() {
        this.status = ExportJobStatus.PENDING;
    }

    public ExportJobStatus getStatus() { return this.status; }
    public void setStatus(ExportJobStatus status) { this.status = status; }

    @Nullable
    public ExportJobConfiguration getConfig() { return this.config; }
    public void setConfig(@Nullable ExportJobConfiguration config) { this.config = config; }

    @Nullable
    public ExportCheckpoint getCheckpoint() { return this.checkpoint; }
    public void setCheckpoint(@Nullable ExportCheckpoint checkpoint) { this.checkpoint = checkpoint; }

    @Nullable
    public Instant getCreatedAt() { return this.createdAt; }
    public void setCreatedAt(@Nullable Instant createdAt) { this.createdAt = createdAt; }

    @Nullable
    public Instant getLastModifiedAt() { return this.lastModifiedAt; }
    public void setLastModifiedAt(@Nullable Instant lastModifiedAt) { this.lastModifiedAt = lastModifiedAt; }

    @Nullable
    public Instant getLastCheckpointTime() { return this.lastCheckpointTime; }
    public void setLastCheckpointTime(@Nullable Instant lastCheckpointTime) { this.lastCheckpointTime = lastCheckpointTime; }

    @Nullable
    public String getLastError() { return this.lastError; }
    public void setLastError(@Nullable String lastError) { this.lastError = lastError; }

    public long getScannedInstances() { return this.scannedInstances; }
    public void setScannedInstances(long scannedInstances) { this.scannedInstances = scannedInstances; }

    public long getExportedInstances() { return this.exportedInstances; }
    public void setExportedInstances(long exportedInstances) { this.exportedInstances = exportedInstances; }

    @Nullable
    public String getOrchestratorInstanceId() { return this.orchestratorInstanceId; }
    public void setOrchestratorInstanceId(@Nullable String orchestratorInstanceId) { this.orchestratorInstanceId = orchestratorInstanceId; }
}
