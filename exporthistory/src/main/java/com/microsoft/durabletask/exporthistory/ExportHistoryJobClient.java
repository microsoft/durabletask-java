// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.FailureDetails;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import com.microsoft.durabletask.TypedEntityMetadata;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeoutException;

/**
 * Client for managing a single export job via entity operations routed through
 * {@link ExecuteExportJobOperationOrchestrator}.
 */
public final class ExportHistoryJobClient {

    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(60);

    private final DurableTaskClient durableTaskClient;
    private final String jobId;
    private final ExportHistoryStorageOptions storageOptions;
    private final EntityInstanceId entityId;

    ExportHistoryJobClient(DurableTaskClient durableTaskClient, String jobId, ExportHistoryStorageOptions storageOptions) {
        if (jobId == null || jobId.isEmpty()) {
            throw new IllegalArgumentException("jobId must not be null or empty.");
        }
        this.durableTaskClient = durableTaskClient;
        this.jobId = jobId;
        this.storageOptions = storageOptions;
        this.entityId = new EntityInstanceId(ExportJob.NAME, jobId);
    }

    /** @return the export job ID. */
    public String getJobId() {
        return this.jobId;
    }

    /**
     * Creates the export job, populating the destination from the registered storage options and waiting for the
     * operation to complete.
     *
     * @param options the creation options
     * @throws ExportJobClientValidationException if creation fails validation or the operation does not complete
     */
    public void create(ExportJobCreationOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null.");
        }

        String existingPrefix = options.getDestination() == null ? null : options.getDestination().getPrefix();
        String defaultPrefix = options.getMode().name().toLowerCase(Locale.ROOT) + "-" + this.jobId + "/";
        String prefix = firstNonNull(existingPrefix, this.storageOptions.getPrefix(), defaultPrefix);
        String container = options.getDestination() == null || options.getDestination().getContainer() == null
                ? this.storageOptions.getContainerName()
                : options.getDestination().getContainer();

        ExportDestination destination = new ExportDestination(container);
        destination.setPrefix(prefix);
        options.setDestination(destination);

        options.validateForCreate();

        ExportJobOperationRequest request = new ExportJobOperationRequest(
                this.entityId, ExportJobTransitions.OP_CREATE, options);

        OrchestrationMetadata result = scheduleAndWait(request);
        if (result.getRuntimeStatus() != OrchestrationRuntimeStatus.COMPLETED) {
            FailureDetails failure = result.getFailureDetails();
            String detail = failure == null ? "" : failure.getErrorMessage();
            throw new ExportJobClientValidationException(
                    "Failed to create export job '" + this.jobId + "': " + detail);
        }
    }

    /**
     * Describes the export job by reading its entity state.
     *
     * @return the export job description
     * @throws ExportJobNotFoundException if the job does not exist
     */
    public ExportJobDescription describe() {
        TypedEntityMetadata<ExportJobState> metadata =
                this.durableTaskClient.getEntities().getEntityMetadata(this.entityId, ExportJobState.class);
        if (metadata == null) {
            throw new ExportJobNotFoundException(this.jobId);
        }
        return ExportJobDescription.fromState(this.jobId, metadata.getState());
    }

    /**
     * Deletes the export job entity. The export orchestrator self-exits on its next cycle once it observes the job
     * is gone.
     */
    public void delete() {
        ExportJobOperationRequest request = new ExportJobOperationRequest(
                this.entityId, ExportJobTransitions.OP_DELETE, null);
        scheduleAndWait(request);
    }

    private OrchestrationMetadata scheduleAndWait(ExportJobOperationRequest request) {
        String instanceId = this.durableTaskClient.scheduleNewOrchestrationInstance(
                ExecuteExportJobOperationOrchestrator.NAME, request);
        try {
            return this.durableTaskClient.waitForInstanceCompletion(instanceId, OPERATION_TIMEOUT, true);
        } catch (TimeoutException e) {
            throw new ExportJobClientValidationException(
                    "Timed out waiting for export job operation on '" + this.jobId + "' to complete.", e);
        }
    }

    private static String firstNonNull(String... values) {
        for (String value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}
