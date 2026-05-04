// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import com.microsoft.durabletask.OrchestrationRuntimeStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Options for creating a new export history job.
 */
public final class ExportJobCreationOptions {

    private static final int MIN_INSTANCES_PER_BATCH = 1;
    private static final int MAX_INSTANCES_PER_BATCH = 1000;
    private static final int DEFAULT_MAX_INSTANCES_PER_BATCH = 100;
    private static final Set<OrchestrationRuntimeStatus> TERMINAL_STATUSES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    OrchestrationRuntimeStatus.COMPLETED,
                    OrchestrationRuntimeStatus.FAILED,
                    OrchestrationRuntimeStatus.TERMINATED)));

    private String jobId;
    private ExportMode mode;
    private Instant completedTimeFrom;
    private Instant completedTimeTo;
    private ExportDestination destination;
    private ExportFormat format;
    private List<OrchestrationRuntimeStatus> runtimeStatus;
    private int maxInstancesPerBatch;

    /**
     * Default constructor for Jackson deserialization.
     */
    public ExportJobCreationOptions() {
        this.maxInstancesPerBatch = DEFAULT_MAX_INSTANCES_PER_BATCH;
    }

    /**
     * Creates new export job creation options.
     *
     * @param jobId               the job ID, or {@code null} to auto-generate
     * @param mode                the export mode (BATCH or CONTINUOUS)
     * @param completedTimeFrom   inclusive start of the completed time window
     * @param completedTimeTo     inclusive end of the completed time window (required for BATCH, null for CONTINUOUS)
     * @param destination         blob storage destination, or {@code null} to use defaults
     * @param format              export format, or {@code null} for default (JSONL+gzip)
     * @param runtimeStatus       runtime status filter, or {@code null} for all terminal statuses
     * @param maxInstancesPerBatch max instances per batch (1-1000, default 100)
     */
    public ExportJobCreationOptions(
            @Nullable String jobId,
            @Nonnull ExportMode mode,
            @Nonnull Instant completedTimeFrom,
            @Nullable Instant completedTimeTo,
            @Nullable ExportDestination destination,
            @Nullable ExportFormat format,
            @Nullable List<OrchestrationRuntimeStatus> runtimeStatus,
            int maxInstancesPerBatch) {

        if (mode == null) {
            throw new IllegalArgumentException("mode must not be null.");
        }
        if (completedTimeFrom == null) {
            throw new IllegalArgumentException("completedTimeFrom must not be null.");
        }
        if (mode == ExportMode.BATCH && completedTimeTo == null) {
            throw new IllegalArgumentException("completedTimeTo is required for BATCH mode.");
        }
        if (mode == ExportMode.CONTINUOUS && completedTimeTo != null) {
            throw new IllegalArgumentException("completedTimeTo must be null for CONTINUOUS mode.");
        }
        if (completedTimeTo != null && !completedTimeTo.isAfter(completedTimeFrom)) {
            throw new IllegalArgumentException("completedTimeTo must be after completedTimeFrom.");
        }
        if (maxInstancesPerBatch < MIN_INSTANCES_PER_BATCH || maxInstancesPerBatch > MAX_INSTANCES_PER_BATCH) {
            throw new IllegalArgumentException(
                    "maxInstancesPerBatch must be between " + MIN_INSTANCES_PER_BATCH +
                    " and " + MAX_INSTANCES_PER_BATCH + ".");
        }
        if (runtimeStatus != null) {
            for (OrchestrationRuntimeStatus status : runtimeStatus) {
                if (!TERMINAL_STATUSES.contains(status)) {
                    throw new IllegalArgumentException(
                            "runtimeStatus must contain only terminal statuses (COMPLETED, FAILED, TERMINATED). Got: " + status);
                }
            }
        }

        this.jobId = jobId != null ? jobId : UUID.randomUUID().toString();
        this.mode = mode;
        this.completedTimeFrom = completedTimeFrom;
        this.completedTimeTo = completedTimeTo;
        this.destination = destination;
        this.format = format != null ? format : ExportFormat.DEFAULT;
        this.runtimeStatus = runtimeStatus;
        this.maxInstancesPerBatch = maxInstancesPerBatch;
    }

    @Nonnull
    public String getJobId() {
        return this.jobId;
    }

    @Nonnull
    public ExportMode getMode() {
        return this.mode;
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
    public ExportDestination getDestination() {
        return this.destination;
    }

    @Nonnull
    public ExportFormat getFormat() {
        return this.format;
    }

    @Nullable
    public List<OrchestrationRuntimeStatus> getRuntimeStatus() {
        return this.runtimeStatus;
    }

    public int getMaxInstancesPerBatch() {
        return this.maxInstancesPerBatch;
    }

    public void setJobId(String jobId) { this.jobId = jobId; }
    public void setMode(ExportMode mode) { this.mode = mode; }
    public void setCompletedTimeFrom(Instant completedTimeFrom) { this.completedTimeFrom = completedTimeFrom; }
    public void setCompletedTimeTo(Instant completedTimeTo) { this.completedTimeTo = completedTimeTo; }
    public void setDestination(ExportDestination destination) { this.destination = destination; }
    public void setFormat(ExportFormat format) { this.format = format; }
    public void setRuntimeStatus(List<OrchestrationRuntimeStatus> runtimeStatus) { this.runtimeStatus = runtimeStatus; }
    public void setMaxInstancesPerBatch(int maxInstancesPerBatch) { this.maxInstancesPerBatch = maxInstancesPerBatch; }
}
