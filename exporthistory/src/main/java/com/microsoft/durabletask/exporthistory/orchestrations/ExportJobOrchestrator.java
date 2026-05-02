// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.orchestrations;

import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOptions;
import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.RetryPolicy;
import com.microsoft.durabletask.exporthistory.constants.ExportJobOperationNames;
import com.microsoft.durabletask.exporthistory.models.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Main orchestrator for export jobs. Manages the batch loop: list terminal instances,
 * export their history, checkpoint progress, and handle retries.
 * <p>
 * Matches the .NET {@code ExportJobOrchestrator} behavior exactly.
 */
public class ExportJobOrchestrator implements TaskOrchestration {

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final int MIN_BACKOFF_SECONDS = 60;
    private static final int MAX_BACKOFF_SECONDS = 300;
    private static final int CONTINUE_AS_NEW_FREQUENCY = 5;
    private static final Duration CONTINUOUS_EXPORT_IDLE_DELAY = Duration.ofMinutes(1);

    // Activity-level retry: 3 attempts, 15s initial, 2x backoff, 60s max
    private static final RetryPolicy EXPORT_ACTIVITY_RETRY_POLICY = new RetryPolicy(
            3, Duration.ofSeconds(15));

    @Override
    public void run(TaskOrchestrationContext ctx) {
        ExportJobRunRequest input = ctx.getInput(ExportJobRunRequest.class);
        if (input == null || input.getJobEntityId() == null) {
            throw new IllegalArgumentException("ExportJobRunRequest with jobEntityId is required.");
        }

        String jobId = input.getJobEntityId().getKey();

        try {
            // Get current job state from entity
            ExportJobState jobState = ctx.callEntity(
                    input.getJobEntityId(),
                    ExportJobOperationNames.GET,
                    ExportJobState.class).await();

            if (jobState == null || jobState.getConfig() == null) {
                throw new IllegalStateException("Export job '" + jobId + "' not found or has no configuration.");
            }
            if (jobState.getStatus() != ExportJobStatus.ACTIVE) {
                return; // Job is no longer active
            }

            ExportJobConfiguration config = jobState.getConfig();
            int processedCycles = input.getProcessedCycles();

            while (true) {
                processedCycles++;
                if (processedCycles > CONTINUE_AS_NEW_FREQUENCY) {
                    ctx.continueAsNew(new ExportJobRunRequest(input.getJobEntityId(), 0));
                    return;
                }

                // Re-check job state on each cycle
                ExportJobState currentState = ctx.callEntity(
                        input.getJobEntityId(),
                        ExportJobOperationNames.GET,
                        ExportJobState.class).await();

                if (currentState == null || currentState.getConfig() == null
                        || currentState.getStatus() != ExportJobStatus.ACTIVE) {
                    return; // Job no longer active
                }

                // List terminal instances
                ListTerminalInstancesRequest listRequest = new ListTerminalInstancesRequest(
                        currentState.getConfig().getFilter().getCompletedTimeFrom(),
                        currentState.getConfig().getFilter().getCompletedTimeTo(),
                        currentState.getConfig().getFilter().getRuntimeStatus(),
                        currentState.getCheckpoint() != null ? currentState.getCheckpoint().getLastInstanceKey() : null,
                        currentState.getConfig().getMaxInstancesPerBatch());

                InstancePage pageResult = ctx.callActivity(
                        "ListTerminalInstancesActivity",
                        listRequest,
                        InstancePage.class).await();

                List<String> instancesToExport = pageResult.getInstanceIds();
                long scannedCount = instancesToExport.size();

                if (scannedCount == 0) {
                    if (config.getMode() == ExportMode.CONTINUOUS) {
                        ctx.createTimer(CONTINUOUS_EXPORT_IDLE_DELAY).await();
                        continue;
                    } else {
                        // BATCH mode — no more instances, complete
                        break;
                    }
                }

                // Process batch with outer retry
                BatchExportResult batchResult = processBatchWithRetry(ctx, input.getJobEntityId(), instancesToExport, config);

                if (batchResult.isAllSucceeded()) {
                    // Commit checkpoint with progress
                    commitCheckpoint(ctx, input.getJobEntityId(),
                            scannedCount, batchResult.getExportedCount(),
                            pageResult.getNextCheckpoint(), null);
                } else {
                    // Failed after all retries — commit without advancing cursor
                    commitCheckpoint(ctx, input.getJobEntityId(),
                            0, 0, null, batchResult.getFailures());
                    throw new IllegalStateException(
                            "Export job '" + jobId + "' batch failed after " + MAX_RETRY_ATTEMPTS + " retry attempts.");
                }
            }

            // Mark completed
            ctx.callEntity(input.getJobEntityId(), ExportJobOperationNames.MARK_AS_COMPLETED).await();

        } catch (Exception ex) {
            // Mark as failed
            try {
                ctx.callEntity(input.getJobEntityId(),
                        ExportJobOperationNames.MARK_AS_FAILED,
                        ex.getMessage(),
                        Void.class).await();
            } catch (Exception ignored) {
                // Best-effort failure marking
            }
            throw ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
        }
    }

    private BatchExportResult processBatchWithRetry(
            TaskOrchestrationContext ctx,
            com.microsoft.durabletask.EntityInstanceId jobEntityId,
            List<String> instanceIds,
            ExportJobConfiguration config) {

        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            List<ExportResult> results = exportBatch(ctx, instanceIds, config);
            List<ExportResult> failedResults = results.stream()
                    .filter(r -> !r.isSuccess())
                    .collect(Collectors.toList());

            if (failedResults.isEmpty()) {
                return new BatchExportResult(true, results.size(), null);
            }

            if (attempt == MAX_RETRY_ATTEMPTS) {
                final int finalAttempt = attempt;
                List<ExportFailure> failures = failedResults.stream()
                        .map(r -> new ExportFailure(
                                r.getInstanceId(),
                                r.getError() != null ? r.getError() : "Unknown error",
                                finalAttempt,
                                ctx.getCurrentInstant()))
                        .collect(Collectors.toList());

                int exportedCount = (int) results.stream().filter(ExportResult::isSuccess).count();
                return new BatchExportResult(false, exportedCount, failures);
            }

            // Exponential backoff: 60s, 120s, 240s (capped at 300s)
            int backoffSeconds = Math.min(MIN_BACKOFF_SECONDS * (int) Math.pow(2, attempt - 1), MAX_BACKOFF_SECONDS);
            ctx.createTimer(Duration.ofSeconds(backoffSeconds)).await();
        }

        return new BatchExportResult(true, 0, null); // Unreachable
    }

    private List<ExportResult> exportBatch(
            TaskOrchestrationContext ctx,
            List<String> instanceIds,
            ExportJobConfiguration config) {

        TaskOptions activityOptions = new TaskOptions(EXPORT_ACTIVITY_RETRY_POLICY);
        List<Task<ExportResult>> exportTasks = new ArrayList<>();

        for (String instanceId : instanceIds) {
            ExportRequest exportRequest = new ExportRequest(
                    instanceId,
                    config.getDestination(),
                    config.getFormat());

            exportTasks.add(ctx.callActivity(
                    "ExportInstanceHistoryActivity",
                    exportRequest,
                    activityOptions,
                    ExportResult.class));
        }

        // Wait for all exports in the batch
        List<ExportResult> results = new ArrayList<>();
        for (Task<ExportResult> task : exportTasks) {
            try {
                results.add(task.await());
            } catch (Exception ex) {
                // Activity failure after all retries — record as failed result
                results.add(new ExportResult("unknown", false, ex.getMessage()));
            }
        }
        return results;
    }

    private void commitCheckpoint(
            TaskOrchestrationContext ctx,
            com.microsoft.durabletask.EntityInstanceId jobEntityId,
            long scannedInstances,
            long exportedInstances,
            ExportCheckpoint checkpoint,
            List<ExportFailure> failures) {

        CommitCheckpointRequest request = new CommitCheckpointRequest(
                scannedInstances, exportedInstances, checkpoint, failures);

        ctx.callEntity(jobEntityId, ExportJobOperationNames.COMMIT_CHECKPOINT,
                request, Void.class).await();
    }
}
