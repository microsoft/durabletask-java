// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.client;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.EntityMetadata;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.exporthistory.constants.ExportHistoryConstants;
import com.microsoft.durabletask.exporthistory.constants.ExportJobOperationNames;
import com.microsoft.durabletask.exporthistory.entity.ExportJob;
import com.microsoft.durabletask.exporthistory.exception.ExportJobNotFoundException;
import com.microsoft.durabletask.exporthistory.models.*;
import com.microsoft.durabletask.exporthistory.options.ExportHistoryStorageOptions;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default per-job client implementation. Delegates to entity operations via
 * the {@code ExecuteExportJobOperationOrchestrator}.
 */
public class DefaultExportHistoryJobClient extends ExportHistoryJobClient {

    private static final Logger logger = Logger.getLogger(DefaultExportHistoryJobClient.class.getName());
    private static final Duration OPERATION_TIMEOUT = Duration.ofMinutes(5);

    private final DurableTaskClient durableTaskClient;
    private final ExportHistoryStorageOptions storageOptions;
    private final EntityInstanceId entityId;

    public DefaultExportHistoryJobClient(
            @Nonnull DurableTaskClient durableTaskClient,
            @Nonnull String jobId,
            @Nonnull ExportHistoryStorageOptions storageOptions) {
        super(jobId);
        this.durableTaskClient = durableTaskClient;
        this.storageOptions = storageOptions;
        this.entityId = new EntityInstanceId(ExportJob.class.getSimpleName(), jobId);
    }

    @Override
    public void create(@Nonnull ExportJobCreationOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null.");
        }

        // Build options with destination from storage options if not set
        ExportDestination destination = options.getDestination();
        if (destination == null) {
            String prefix = options.getMode().name().toLowerCase() + "-" + this.jobId + "/";
            destination = new ExportDestination(
                    this.storageOptions.getContainerName(),
                    this.storageOptions.getPrefix() != null ? this.storageOptions.getPrefix() : prefix);
        }

        // Create options with resolved destination
        ExportJobCreationOptions resolvedOptions = new ExportJobCreationOptions(
                options.getJobId(),
                options.getMode(),
                options.getCompletedTimeFrom(),
                options.getCompletedTimeTo(),
                destination,
                options.getFormat(),
                options.getRuntimeStatus(),
                options.getMaxInstancesPerBatch());

        // Schedule the operation via ExecuteExportJobOperationOrchestrator
        ExportJobOperationRequest request = new ExportJobOperationRequest(
                this.entityId, ExportJobOperationNames.CREATE, resolvedOptions);

        String instanceId = this.durableTaskClient.scheduleNewOrchestrationInstance(
                "ExecuteExportJobOperationOrchestrator", request);

        // Wait for completion
        try {
            OrchestrationMetadata result = this.durableTaskClient.waitForInstanceCompletion(
                    instanceId, OPERATION_TIMEOUT, true);
            if (result != null && result.isRunning()) {
                throw new RuntimeException("Create operation did not complete in time for job: " + this.jobId);
            }
            if (result != null && result.getRuntimeStatus() == com.microsoft.durabletask.OrchestrationRuntimeStatus.FAILED) {
                String details = result.getFailureDetails() != null
                        ? result.getFailureDetails().getErrorMessage()
                        : "unknown error";
                throw new RuntimeException("Create operation failed for job '" + this.jobId + "': " + details);
            }
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out waiting for create operation for job: " + this.jobId, e);
        }

        // Wait for entity state to become visible (eventual consistency)
        for (int i = 0; i < 10; i++) {
            EntityMetadata metadata = this.durableTaskClient.getEntities()
                    .getEntityMetadata(this.entityId, false);
            if (metadata != null) {
                return;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted waiting for entity visibility", ie);
            }
        }
        throw new RuntimeException(
                "Create operation completed but entity metadata for job '" + this.jobId
                        + "' did not become visible within the expected time.");
    }

    @Override
    @Nonnull
    public ExportJobDescription describe() {
        EntityMetadata metadata = this.durableTaskClient.getEntities()
                .getEntityMetadata(this.entityId, true);

        if (metadata == null) {
            throw new ExportJobNotFoundException(this.jobId);
        }

        ExportJobState state = metadata.readStateAs(ExportJobState.class);
        if (state == null) {
            throw new ExportJobNotFoundException(this.jobId);
        }

        ExportJobDescription desc = new ExportJobDescription();
        desc.setJobId(this.jobId);
        desc.setStatus(state.getStatus());
        desc.setCreatedAt(state.getCreatedAt());
        desc.setLastModifiedAt(state.getLastModifiedAt());
        desc.setConfig(state.getConfig());
        desc.setOrchestratorInstanceId(state.getOrchestratorInstanceId());
        desc.setScannedInstances(state.getScannedInstances());
        desc.setExportedInstances(state.getExportedInstances());
        desc.setLastError(state.getLastError());
        desc.setCheckpoint(state.getCheckpoint());
        desc.setLastCheckpointTime(state.getLastCheckpointTime());
        return desc;
    }

    @Override
    public void delete() {
        logger.info("Deleting export job: " + this.jobId);

        // Step 1: Delete the entity via orchestrator
        ExportJobOperationRequest deleteRequest = new ExportJobOperationRequest(
                this.entityId, ExportJobOperationNames.DELETE);
        this.durableTaskClient.scheduleNewOrchestrationInstance(
                "ExecuteExportJobOperationOrchestrator", deleteRequest);

        // Step 2: Terminate and purge the linked export orchestration
        String orchestrationInstanceId = ExportHistoryConstants.getOrchestratorInstanceId(this.jobId);
        terminateAndPurgeOrchestration(orchestrationInstanceId);
    }

    private void terminateAndPurgeOrchestration(String orchestrationInstanceId) {
        try {
            this.durableTaskClient.terminate(orchestrationInstanceId, "Export job deleted");

            try {
                this.durableTaskClient.waitForInstanceCompletion(
                        orchestrationInstanceId, OPERATION_TIMEOUT, false);
            } catch (TimeoutException e) {
                logger.log(Level.WARNING,
                        "Timed out waiting for orchestration termination: " + orchestrationInstanceId, e);
            }

            this.durableTaskClient.purgeInstance(orchestrationInstanceId);
        } catch (IllegalArgumentException e) {
            // Instance not found — already deleted or never existed
            logger.info("Orchestration instance '" + orchestrationInstanceId +
                    "' is already purged or never existed.");
        } catch (Exception e) {
            logger.log(Level.WARNING,
                    "Failed to terminate/purge orchestration: " + orchestrationInstanceId, e);
        }
    }
}
