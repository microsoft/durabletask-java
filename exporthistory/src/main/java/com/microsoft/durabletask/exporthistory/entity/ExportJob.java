// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.entity;

import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.NewOrchestrationInstanceOptions;
import com.microsoft.durabletask.TaskEntity;
import com.microsoft.durabletask.TaskEntityContext;
import com.microsoft.durabletask.TaskEntityOperation;
import com.microsoft.durabletask.exporthistory.constants.ExportHistoryConstants;
import com.microsoft.durabletask.exporthistory.constants.ExportJobOperationNames;
import com.microsoft.durabletask.exporthistory.exception.ExportJobInvalidTransitionException;
import com.microsoft.durabletask.exporthistory.models.*;

import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Durable entity that manages an export history job: lifecycle, configuration, and progress.
 */
public class ExportJob implements TaskEntity {

    private static final Logger logger = Logger.getLogger(ExportJob.class.getName());

    @Override
    public Object run(TaskEntityOperation operation) throws Exception {
        switch (operation.getName()) {
            case ExportJobOperationNames.CREATE:
                return create(operation);
            case ExportJobOperationNames.GET:
                return get(operation);
            case ExportJobOperationNames.RUN:
                return runExport(operation);
            case ExportJobOperationNames.COMMIT_CHECKPOINT:
                return commitCheckpoint(operation);
            case ExportJobOperationNames.MARK_AS_COMPLETED:
                return markAsCompleted(operation);
            case ExportJobOperationNames.MARK_AS_FAILED:
                return markAsFailed(operation);
            case ExportJobOperationNames.DELETE:
                return delete(operation);
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation.getName());
        }
    }

    private Object create(TaskEntityOperation operation) {
        ExportJobState state = getOrCreateState(operation);
        ExportJobCreationOptions options = operation.getInput(ExportJobCreationOptions.class);
        if (options == null) {
            throw new IllegalArgumentException("ExportJobCreationOptions input is required for Create.");
        }

        if (!ExportJobTransitions.isValidTransition(ExportJobOperationNames.CREATE, state.getStatus(), ExportJobStatus.ACTIVE)) {
            throw new ExportJobInvalidTransitionException(
                    options.getJobId(), state.getStatus(), ExportJobStatus.ACTIVE, ExportJobOperationNames.CREATE);
        }

        // Build configuration from creation options
        ExportJobConfiguration config = new ExportJobConfiguration(
                options.getMode(),
                new ExportFilter(
                        options.getCompletedTimeFrom(),
                        options.getCompletedTimeTo(),
                        options.getRuntimeStatus()),
                options.getDestination(),
                options.getFormat(),
                options.getMaxInstancesPerBatch());

        state.setConfig(config);
        state.setStatus(ExportJobStatus.ACTIVE);
        Instant now = Instant.now();
        state.setCreatedAt(now);
        state.setLastModifiedAt(now);
        state.setLastError(null);
        state.setScannedInstances(0);
        state.setExportedInstances(0);
        state.setCheckpoint(null);
        state.setLastCheckpointTime(null);

        operation.getState().setState(state);

        logger.info("Created export job: " + options.getJobId());

        // Self-signal to start the Run operation on a fresh dispatch
        TaskEntityContext ctx = operation.getContext();
        ctx.signalEntity(ctx.getId(), ExportJobOperationNames.RUN);

        return null;
    }

    private Object get(TaskEntityOperation operation) {
        return getOrCreateState(operation);
    }

    private Object runExport(TaskEntityOperation operation) {
        ExportJobState state = getOrCreateState(operation);
        if (state.getConfig() == null) {
            throw new IllegalStateException("Export job has no configuration.");
        }
        if (state.getStatus() != ExportJobStatus.ACTIVE) {
            throw new IllegalStateException("Export job must be in Active status to run.");
        }

        // Start the export orchestration with a deterministic instance ID
        TaskEntityContext ctx = operation.getContext();
        String orchestratorInstanceId = ExportHistoryConstants.getOrchestratorInstanceId(ctx.getId().getKey());
        NewOrchestrationInstanceOptions startOptions = new NewOrchestrationInstanceOptions()
                .setInstanceId(orchestratorInstanceId);

        ctx.startNewOrchestration(
                "ExportJobOrchestrator",
                new ExportJobRunRequest(ctx.getId()),
                startOptions);

        state.setOrchestratorInstanceId(orchestratorInstanceId);
        state.setLastModifiedAt(Instant.now());
        operation.getState().setState(state);

        logger.info("Started export orchestrator: " + orchestratorInstanceId);
        return null;
    }

    private Object commitCheckpoint(TaskEntityOperation operation) {
        ExportJobState state = getOrCreateState(operation);
        CommitCheckpointRequest request = operation.getInput(CommitCheckpointRequest.class);
        if (request == null) {
            throw new IllegalArgumentException("CommitCheckpointRequest input is required.");
        }

        state.setScannedInstances(state.getScannedInstances() + request.getScannedInstances());
        state.setExportedInstances(state.getExportedInstances() + request.getExportedInstances());

        if (request.getCheckpoint() != null) {
            state.setCheckpoint(request.getCheckpoint());
        }

        Instant now = Instant.now();
        state.setLastCheckpointTime(now);
        state.setLastModifiedAt(now);

        // If failures occurred and checkpoint is null (batch failed), mark as failed
        if (request.getCheckpoint() == null && request.getFailures() != null && !request.getFailures().isEmpty()) {
            state.setStatus(ExportJobStatus.FAILED);
            StringBuilder sb = new StringBuilder("Batch export failed. Failures: ");
            for (ExportFailure f : request.getFailures()) {
                sb.append(f.getInstanceId()).append(": ").append(f.getReason()).append("; ");
            }
            state.setLastError(sb.toString());
        }

        operation.getState().setState(state);
        return null;
    }

    private Object markAsCompleted(TaskEntityOperation operation) {
        ExportJobState state = getOrCreateState(operation);
        String jobId = operation.getContext().getId().getKey();

        if (!ExportJobTransitions.isValidTransition(ExportJobOperationNames.MARK_AS_COMPLETED, state.getStatus(), ExportJobStatus.COMPLETED)) {
            throw new ExportJobInvalidTransitionException(
                    jobId, state.getStatus(), ExportJobStatus.COMPLETED, ExportJobOperationNames.MARK_AS_COMPLETED);
        }

        state.setStatus(ExportJobStatus.COMPLETED);
        state.setLastModifiedAt(Instant.now());
        state.setLastError(null);
        operation.getState().setState(state);

        logger.info("Export job marked as completed: " + jobId);
        return null;
    }

    private Object markAsFailed(TaskEntityOperation operation) {
        ExportJobState state = getOrCreateState(operation);
        String jobId = operation.getContext().getId().getKey();
        String errorMessage = operation.getInput(String.class);

        if (!ExportJobTransitions.isValidTransition(ExportJobOperationNames.MARK_AS_FAILED, state.getStatus(), ExportJobStatus.FAILED)) {
            throw new ExportJobInvalidTransitionException(
                    jobId, state.getStatus(), ExportJobStatus.FAILED, ExportJobOperationNames.MARK_AS_FAILED);
        }

        state.setStatus(ExportJobStatus.FAILED);
        state.setLastError(errorMessage);
        state.setLastModifiedAt(Instant.now());
        operation.getState().setState(state);

        logger.info("Export job marked as failed: " + jobId + " - " + errorMessage);
        return null;
    }

    private Object delete(TaskEntityOperation operation) {
        logger.info("Deleting export job entity: " + operation.getContext().getId().getKey());
        // Clear entity state (standard entity deletion pattern)
        operation.getState().setState(null);
        return null;
    }

    private ExportJobState getOrCreateState(TaskEntityOperation operation) {
        ExportJobState state = operation.getState().getState(ExportJobState.class);
        if (state == null) {
            state = new ExportJobState();
        }
        return state;
    }
}
