// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
/**
 * Configuration for creating an export job.
 * <p>
 * Export supports <em>terminal</em> orchestration statuses only ({@link OrchestrationRuntimeStatus#COMPLETED},
 * {@link OrchestrationRuntimeStatus#FAILED}, {@link OrchestrationRuntimeStatus#TERMINATED}); when no status filter
 * is supplied, all three are exported.
 *
 * <pre>{@code
 * export.createJob(new ExportJobCreationOptions("nightly-archive")
 *     .setMode(ExportMode.BATCH)
 *     .setCompletedTimeFrom(Instant.parse("2026-06-01T00:00:00Z"))
 *     .setCompletedTimeTo(Instant.parse("2026-06-25T00:00:00Z"))
 *     .setRuntimeStatus(Collections.singletonList(OrchestrationRuntimeStatus.COMPLETED))
 *     .setMaxInstancesPerBatch(200));
 * }</pre>
 */
public final class ExportJobCreationOptions {

    /** The lowest valid value for {@link #setMaxInstancesPerBatch(int)}. */
    public static final int MIN_INSTANCES_PER_BATCH = 1;

    /** The highest valid value for {@link #setMaxInstancesPerBatch(int)}. */
    public static final int MAX_INSTANCES_PER_BATCH = 1000;

    private static final List<OrchestrationRuntimeStatus> TERMINAL_STATUSES = Collections.unmodifiableList(
            Arrays.asList(
                    OrchestrationRuntimeStatus.COMPLETED,
                    OrchestrationRuntimeStatus.FAILED,
                    OrchestrationRuntimeStatus.TERMINATED));

    private final String jobId;
    private ExportMode mode = ExportMode.BATCH;
    private Instant completedTimeFrom;
    private Instant completedTimeTo;
    private List<OrchestrationRuntimeStatus> runtimeStatus = new ArrayList<>(TERMINAL_STATUSES);
    private int maxInstancesPerBatch = 100;
    private ExportFormat format = ExportFormat.getDefault();
    private ExportDestination destination;

    /**
     * Creates a new {@code ExportJobCreationOptions} with the given job ID.
     *
     * @param jobId the unique export job ID; if {@code null} or empty, a random ID is generated
     */
    @JsonCreator
    public ExportJobCreationOptions(@JsonProperty("jobId") @Nullable String jobId) {
        this.jobId = (jobId == null || jobId.isEmpty()) ? UUID.randomUUID().toString().replace("-", "") : jobId;
    }

    /** @return the export job ID. */
    public String getJobId() {
        return this.jobId;
    }

    /** @return the export mode. */
    public ExportMode getMode() {
        return this.mode;
    }

    /**
     * Sets the export mode (default {@link ExportMode#BATCH}).
     *
     * @param mode the export mode
     * @return this options object
     */
    public ExportJobCreationOptions setMode(ExportMode mode) {
        if (mode == null) {
            throw new IllegalArgumentException("mode must not be null.");
        }
        this.mode = mode;
        return this;
    }

    /** @return the inclusive completion-time lower bound, or {@code null} if not set. */
    @Nullable
    public Instant getCompletedTimeFrom() {
        return this.completedTimeFrom;
    }

    /**
     * Sets the inclusive completion-time lower bound. Required for {@link ExportMode#BATCH}; for
     * {@link ExportMode#CONTINUOUS} it defaults to the job creation time when omitted.
     *
     * @param completedTimeFrom the lower bound, or {@code null} to clear
     * @return this options object
     */
    public ExportJobCreationOptions setCompletedTimeFrom(@Nullable Instant completedTimeFrom) {
        this.completedTimeFrom = completedTimeFrom;
        return this;
    }

    /** @return the inclusive completion-time upper bound, or {@code null} if not set. */
    @Nullable
    public Instant getCompletedTimeTo() {
        return this.completedTimeTo;
    }

    /**
     * Sets the inclusive completion-time upper bound. Required for {@link ExportMode#BATCH}; not allowed for
     * {@link ExportMode#CONTINUOUS}.
     *
     * @param completedTimeTo the upper bound, or {@code null} to clear
     * @return this options object
     */
    public ExportJobCreationOptions setCompletedTimeTo(@Nullable Instant completedTimeTo) {
        this.completedTimeTo = completedTimeTo;
        return this;
    }

    /** @return an unmodifiable view of the terminal runtime statuses to export. */
    public List<OrchestrationRuntimeStatus> getRuntimeStatus() {
        return Collections.unmodifiableList(this.runtimeStatus);
    }

    /**
     * Sets the terminal runtime statuses to filter by. Only {@link OrchestrationRuntimeStatus#COMPLETED},
     * {@link OrchestrationRuntimeStatus#FAILED}, and {@link OrchestrationRuntimeStatus#TERMINATED} are permitted.
     * Passing {@code null} or an empty list resets to all terminal statuses.
     *
     * @param runtimeStatus the terminal runtime statuses, or {@code null}/empty for all terminal statuses
     * @return this options object
     * @throws IllegalArgumentException if any status is non-terminal
     */
    public ExportJobCreationOptions setRuntimeStatus(@Nullable List<OrchestrationRuntimeStatus> runtimeStatus) {
        if (runtimeStatus == null || runtimeStatus.isEmpty()) {
            this.runtimeStatus = new ArrayList<>(TERMINAL_STATUSES);
            return this;
        }
        for (OrchestrationRuntimeStatus status : runtimeStatus) {
            if (!TERMINAL_STATUSES.contains(status)) {
                throw new IllegalArgumentException(
                        "Export supports terminal orchestration statuses only. Valid statuses are: "
                                + "COMPLETED, FAILED, and TERMINATED.");
            }
        }
        this.runtimeStatus = new ArrayList<>(runtimeStatus);
        return this;
    }

    /** @return the maximum number of instances fetched per batch. */
    public int getMaxInstancesPerBatch() {
        return this.maxInstancesPerBatch;
    }
    /**
     * Sets the maximum number of instances to fetch per batch (default 100).
     *
     * @param maxInstancesPerBatch a value in the range [{@value #MIN_INSTANCES_PER_BATCH},
     *                             {@value #MAX_INSTANCES_PER_BATCH}]
     * @return this options object
     * @throws IllegalArgumentException if the value is out of range
     */
    public ExportJobCreationOptions setMaxInstancesPerBatch(int maxInstancesPerBatch) {
        if (maxInstancesPerBatch < MIN_INSTANCES_PER_BATCH || maxInstancesPerBatch > MAX_INSTANCES_PER_BATCH) {
            throw new IllegalArgumentException(
                    "MaxInstancesPerBatch must be between " + MIN_INSTANCES_PER_BATCH + " and "
                            + MAX_INSTANCES_PER_BATCH + ".");
        }
        this.maxInstancesPerBatch = maxInstancesPerBatch;
        return this;
    }

    /** @return the export format (default JSONL + gzip). */
    public ExportFormat getFormat() {
        return this.format;
    }

    /**
     * Sets the export format.
     *
     * @param format the export format
     * @return this options object
     */
    public ExportJobCreationOptions setFormat(ExportFormat format) {
        if (format == null) {
            throw new IllegalArgumentException("format must not be null.");
        }
        this.format = format;
        return this;
    }

    /**
     * Gets the export destination. This is populated by the client from its registered
     * {@link ExportHistoryStorageOptions} before the job is created; callers do not normally set it.
     *
     * @return the export destination, or {@code null} if not yet populated
     */
    @Nullable
    public ExportDestination getDestination() {
        return this.destination;
    }

    /**
     * Sets the export destination. Normally called by the client, not the user.
     *
     * @param destination the export destination
     * @return this options object
     */
    public ExportJobCreationOptions setDestination(@Nullable ExportDestination destination) {
        this.destination = destination;
        return this;
    }

    /**
     * Validates mode-specific completion-window rules. Intended to be called
     * by the client at job-creation time.
     *
     * @throws IllegalArgumentException if the configuration is invalid for the selected mode
     */
    public void validateForCreate() {
        if (this.mode == ExportMode.BATCH) {
            if (this.completedTimeFrom == null) {
                throw new IllegalArgumentException("CompletedTimeFrom is required for BATCH export mode.");
            }
            if (this.completedTimeTo == null) {
                throw new IllegalArgumentException("CompletedTimeTo is required for BATCH export mode.");
            }
            if (!this.completedTimeTo.isAfter(this.completedTimeFrom)) {
                throw new IllegalArgumentException(
                        "CompletedTimeTo must be greater than CompletedTimeFrom for BATCH export mode.");
            }
            if (this.completedTimeTo.isAfter(Instant.now())) {
                throw new IllegalArgumentException("CompletedTimeTo cannot be in the future.");
            }
        } else if (this.mode == ExportMode.CONTINUOUS) {
            if (this.completedTimeTo != null) {
                throw new IllegalArgumentException("CompletedTimeTo is not allowed for CONTINUOUS export mode.");
            }
        } else {
            throw new IllegalArgumentException("Invalid export mode.");
        }
    }
}
