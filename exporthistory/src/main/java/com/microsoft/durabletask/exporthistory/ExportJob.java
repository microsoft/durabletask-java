// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.NewOrchestrationInstanceOptions;
import com.microsoft.durabletask.TaskEntityOperation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Durable entity that manages a history export job: lifecycle, configuration, checkpoint, and progress.
 * <p>
 * Operations are dispatched by method name (case-insensitive): {@code Create}, {@code Get}, {@code Run},
 * {@code CommitCheckpoint}, {@code MarkAsCompleted}, {@code MarkAsFailed}, {@code Delete}.
 */
public final class ExportJob extends AbstractTaskEntity<ExportJobState> {

    /** The registered entity name. */
    public static final String NAME = "ExportJob";

    private static final Logger LOGGER = Logger.getLogger(ExportJob.class.getName());

    @Override
    protected Class<ExportJobState> getStateType() {
        return ExportJobState.class;
    }

    @Override
    protected ExportJobState initializeState(TaskEntityOperation operation) {
        ExportJobState state = new ExportJobState();
        state.setStatus(ExportJobStatus.PENDING);
        return state;
    }

    /**
     * Creates the export job from creation options and signals the {@code Run} operation to start the export.
     *
     * @param creationOptions the creation options (with destination populated by the client)
     */
    public void create(ExportJobCreationOptions creationOptions) {
        if (creationOptions == null) {
            throw new IllegalArgumentException("creationOptions must not be null.");
        }
        if (!ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_CREATE, this.state.getStatus(), ExportJobStatus.ACTIVE)) {
            throw new ExportJobInvalidTransitionException(
                    creationOptions.getJobId(),
                    this.state.getStatus(),
                    ExportJobStatus.ACTIVE,
                    ExportJobTransitions.OP_CREATE);
        }
        if (creationOptions.getDestination() == null) {
            throw new IllegalStateException("Export destination must be populated before reaching the entity.");
        }

        List<com.microsoft.durabletask.OrchestrationRuntimeStatus> statuses =
                creationOptions.getRuntimeStatus() == null
                        ? null
                        : new ArrayList<>(creationOptions.getRuntimeStatus());
        ExportFilter filter = new ExportFilter(
                creationOptions.getCompletedTimeFrom(),
                creationOptions.getCompletedTimeTo(),
                statuses);
        ExportJobConfiguration config = new ExportJobConfiguration(
                creationOptions.getMode(),
                filter,
                creationOptions.getDestination(),
                creationOptions.getFormat(),
                creationOptions.getMaxInstancesPerBatch());

        Instant now = Instant.now();
        this.state.setConfig(config);
        this.state.setStatus(ExportJobStatus.ACTIVE);
        this.state.setCreatedAt(now);
        this.state.setLastModifiedAt(now);
        this.state.setLastError(null);
        this.state.setScannedInstances(0);
        this.state.setExportedInstances(0);
        this.state.setCheckpoint(null);
        this.state.setLastCheckpointTime(null);

        // Signal Run to start the export orchestration.
        this.context.signalEntity(this.context.getId(), ExportJobTransitions.OP_RUN);
        LOGGER.log(Level.INFO, "Created export job {0}", creationOptions.getJobId());
    }

    /**
     * Gets the current state of the export job.
     *
     * @return the current export job state
     */
    public ExportJobState get() {
        return this.state;
    }

    /**
     * Starts the export orchestration. Requires the job to be in the {@link ExportJobStatus#ACTIVE} state.
     */
    public void run() {
        if (this.state.getConfig() == null) {
            throw new IllegalStateException("Export job configuration must be set before running.");
        }
        if (this.state.getStatus() != ExportJobStatus.ACTIVE) {
            throw new IllegalStateException("Export job must be in ACTIVE status to run.");
        }

        try {
            String instanceId = ExportHistoryConstants.getOrchestratorInstanceId(this.context.getId().getKey());
            this.context.startNewOrchestration(
                    ExportJobOrchestrator.NAME,
                    new ExportJobRunRequest(this.context.getId(), 0),
                    new NewOrchestrationInstanceOptions().setInstanceId(instanceId));
            this.state.setOrchestratorInstanceId(instanceId);
            this.state.setLastModifiedAt(Instant.now());
        } catch (RuntimeException ex) {
            this.state.setStatus(ExportJobStatus.FAILED);
            this.state.setLastError(ex.getMessage());
            this.state.setLastModifiedAt(Instant.now());
            LOGGER.log(Level.WARNING, "Failed to start export orchestration for job "
                    + this.context.getId().getKey(), ex);
        }
    }

    /**
     * Commits a checkpoint snapshot with progress updates and optional failures.
     *
     * @param request the checkpoint commit request
     */
    public void commitCheckpoint(CommitCheckpointRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null.");
        }

        this.state.setScannedInstances(this.state.getScannedInstances() + request.getScannedInstances());
        this.state.setExportedInstances(this.state.getExportedInstances() + request.getExportedInstances());

        if (request.getCheckpoint() != null) {
            this.state.setCheckpoint(request.getCheckpoint());
        }

        Instant now = Instant.now();
        this.state.setLastCheckpointTime(now);
        this.state.setLastModifiedAt(now);

        if (request.getCheckpoint() == null
                && request.getFailures() != null
                && !request.getFailures().isEmpty()) {
            this.state.setStatus(ExportJobStatus.FAILED);
            String failureSummary = request.getFailures().stream()
                    .map(f -> f.getInstanceId() + ": " + f.getReason())
                    .collect(Collectors.joining("; "));
            this.state.setLastError("Batch export failed after retries. Failures: " + failureSummary);
        }
    }

    /**
     * Marks the export job as completed. Requires the job to be in the {@link ExportJobStatus#ACTIVE} state.
     */
    public void markAsCompleted() {
        if (!ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_MARK_AS_COMPLETED, this.state.getStatus(), ExportJobStatus.COMPLETED)) {
            throw new ExportJobInvalidTransitionException(
                    this.context.getId().getKey(),
                    this.state.getStatus(),
                    ExportJobStatus.COMPLETED,
                    ExportJobTransitions.OP_MARK_AS_COMPLETED);
        }
        this.state.setStatus(ExportJobStatus.COMPLETED);
        this.state.setLastModifiedAt(Instant.now());
        this.state.setLastError(null);
    }

    /**
     * Marks the export job as failed. Requires the job to be in the {@link ExportJobStatus#ACTIVE} state.
     *
     * @param errorMessage the error message describing why the job failed
     */
    public void markAsFailed(String errorMessage) {
        if (!ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_MARK_AS_FAILED, this.state.getStatus(), ExportJobStatus.FAILED)) {
            throw new ExportJobInvalidTransitionException(
                    this.context.getId().getKey(),
                    this.state.getStatus(),
                    ExportJobStatus.FAILED,
                    ExportJobTransitions.OP_MARK_AS_FAILED);
        }
        this.state.setStatus(ExportJobStatus.FAILED);
        this.state.setLastError(errorMessage);
        this.state.setLastModifiedAt(Instant.now());
    }

    /**
     * Deletes the export job entity by clearing its state. Deleting an active job stops its orchestrator, which
     * exits on its next cycle when it observes the job is no longer active.
     */
    public void delete() {
        this.state = null;
    }
}
