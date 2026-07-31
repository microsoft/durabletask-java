// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.RetryPolicy;
import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOptions;
import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.interruption.ContinueAsNewInterruption;
import com.microsoft.durabletask.interruption.OrchestratorBlockedException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Orchestrator that performs the export work: it pages terminal instances, fans out per-instance export activities,
 * commits checkpoints to the {@link ExportJob} entity, and handles BATCH vs CONTINUOUS modes with bounded retries
 * and periodic {@code continueAsNew}.
 */
final class ExportJobOrchestrator implements TaskOrchestration {

    /** The registered orchestration name. */
    public static final String NAME = "ExportJobOrchestrator";

    private static final Logger LOGGER = Logger.getLogger(ExportJobOrchestrator.class.getName());

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final int MIN_BACKOFF_SECONDS = 60;
    private static final int MAX_BACKOFF_SECONDS = 300;
    private static final int CONTINUE_AS_NEW_FREQUENCY = 5;
    private static final Duration CONTINUOUS_EXPORT_IDLE_DELAY = Duration.ofMinutes(1);

    private static final TaskOptions EXPORT_ACTIVITY_RETRY_OPTIONS = new TaskOptions(
            new RetryPolicy(3, Duration.ofSeconds(15))
                    .setBackoffCoefficient(2.0)
                    .setMaxRetryInterval(Duration.ofSeconds(60)));

    @Override
    public void run(TaskOrchestrationContext ctx) {
        ExportJobRunRequest input = ctx.getInput(ExportJobRunRequest.class);
        EntityInstanceId jobEntityId = input.getJobEntityId();
        String jobId = jobEntityId.getKey();
        log(ctx, Level.INFO, "Export orchestrator started for job " + jobId);

        try {
            ExportJobState jobState = callGet(ctx, jobEntityId);
            if (jobState == null || jobState.getConfig() == null) {
                throw new IllegalStateException(
                        "Export job '" + jobEntityId.getKey() + "' not found or has no configuration.");
            }
            if (jobState.getStatus() != ExportJobStatus.ACTIVE) {
                // Job is no longer active (deleted, completed, or failed) - nothing to do.
                return;
            }

            ExportJobConfiguration config = jobState.getConfig();
            int processedCycles = input.getProcessedCycles();

            while (true) {
                processedCycles++;
                if (processedCycles > CONTINUE_AS_NEW_FREQUENCY) {
                    ctx.continueAsNew(new ExportJobRunRequest(jobEntityId, 0));
                    return;
                }

                ExportJobState currentState = callGet(ctx, jobEntityId);
                if (currentState == null
                        || currentState.getConfig() == null
                        || currentState.getStatus() != ExportJobStatus.ACTIVE) {
                    return;
                }

                ExportFilter filter = currentState.getConfig().getFilter();
                String lastInstanceKey = currentState.getCheckpoint() == null
                        ? null
                        : currentState.getCheckpoint().getLastInstanceKey();
                ListTerminalInstancesRequest listRequest = new ListTerminalInstancesRequest(
                        filter.getCompletedTimeFrom(),
                        filter.getCompletedTimeTo(),
                        filter.getRuntimeStatus(),
                        lastInstanceKey,
                        currentState.getConfig().getMaxInstancesPerBatch());

                InstancePage pageResult = ctx.callActivity(
                        ListTerminalInstancesActivity.NAME, listRequest, InstancePage.class).await();

                List<String> instancesToExport = pageResult.getInstanceIds();
                long scannedCount = instancesToExport.size();

                if (scannedCount == 0) {
                    if (config.getMode() == ExportMode.CONTINUOUS) {
                        ctx.createTimer(CONTINUOUS_EXPORT_IDLE_DELAY).await();
                        continue;
                    } else if (config.getMode() == ExportMode.BATCH) {
                        break;
                    } else {
                        throw new IllegalStateException("Invalid export mode.");
                    }
                }

                BatchExportResult batchResult = processBatchWithRetry(ctx, instancesToExport, config);

                if (batchResult.allSucceeded) {
                    commitCheckpoint(
                            ctx, jobEntityId, scannedCount, batchResult.exportedCount,
                            pageResult.getNextCheckpoint(), null);
                    log(ctx, Level.INFO, "Job " + jobId + " batch scanned=" + scannedCount
                            + " exported=" + batchResult.exportedCount);
                } else {
                    commitCheckpoint(ctx, jobEntityId, 0, 0, null, batchResult.failures);
                    throw new IllegalStateException("Export job '" + jobId + "' batch export failed after "
                            + MAX_RETRY_ATTEMPTS + " retry attempts. " + summarizeFailures(batchResult.failures));
                }
            }

            markAsCompleted(ctx, jobEntityId);
            log(ctx, Level.INFO, "Export orchestrator completed for job " + jobId);
        } catch (OrchestratorBlockedException | ContinueAsNewInterruption controlFlow) {
            // These are the SDK's control-flow signals (await yield / continueAsNew). They must never be
            // treated as failures - rethrow so the runtime can suspend/restart the orchestration.
            throw controlFlow;
        } catch (RuntimeException ex) {
            // A genuine, unexpected failure while the job is still active. Mark the job failed, then fail the
            // orchestration. (The await inside markAsFailed may itself yield; that interruption propagates.)
            log(ctx, Level.WARNING, "Export orchestrator failed for job " + jobId + ": " + ex.getMessage());
            markAsFailed(ctx, jobEntityId, ex.getMessage());
            throw ex;
        }
    }

    private static void log(TaskOrchestrationContext ctx, Level level, String message) {
        if (!ctx.getIsReplaying()) {
            LOGGER.log(level, message);
        }
    }

    private static ExportJobState callGet(TaskOrchestrationContext ctx, EntityInstanceId jobEntityId) {
        return ctx.getEntities()
                .callEntity(jobEntityId, ExportJobTransitions.OP_GET, null, ExportJobState.class)
                .await();
    }

    private BatchExportResult processBatchWithRetry(
            TaskOrchestrationContext ctx,
            List<String> instanceIds,
            ExportJobConfiguration config) {
        // Retries until the batch fully succeeds or MAX_RETRY_ATTEMPTS is reached; every path returns from inside.
        for (int attempt = 1; ; attempt++) {
            List<ExportResult> results = exportBatch(ctx, instanceIds, config);
            List<ExportResult> failedResults = results.stream()
                    .filter(r -> !r.isSuccess())
                    .collect(Collectors.toList());

            if (failedResults.isEmpty()) {
                return BatchExportResult.succeeded(results.size());
            }

            if (attempt >= MAX_RETRY_ATTEMPTS) {
                Instant now = ctx.getCurrentInstant();
                int finalAttempt = attempt;
                List<ExportFailure> failures = failedResults.stream()
                        .map(r -> new ExportFailure(
                                r.getInstanceId(),
                                r.getError() == null ? "Unknown error" : r.getError(),
                                finalAttempt,
                                now))
                        .collect(Collectors.toList());
                long exportedCount = results.stream().filter(ExportResult::isSuccess).count();
                return BatchExportResult.failed((int) exportedCount, failures);
            }

            int backoffSeconds = Math.min(
                    MIN_BACKOFF_SECONDS * (int) Math.pow(2, attempt - 1), MAX_BACKOFF_SECONDS);
            ctx.createTimer(Duration.ofSeconds(backoffSeconds)).await();
        }
    }

    private List<ExportResult> exportBatch(
            TaskOrchestrationContext ctx,
            List<String> instanceIds,
            ExportJobConfiguration config) {
        List<ExportResult> results = new ArrayList<>();
        List<Task<ExportResult>> exportTasks = new ArrayList<>();

        for (String instanceId : instanceIds) {
            ExportRequest exportRequest = new ExportRequest(
                    instanceId, config.getDestination(), config.getFormat());
            exportTasks.add(ctx.callActivity(
                    ExportInstanceHistoryActivity.NAME,
                    exportRequest,
                    EXPORT_ACTIVITY_RETRY_OPTIONS,
                    ExportResult.class));

            if (exportTasks.size() >= config.getMaxParallelExports()) {
                results.addAll(ctx.allOf(exportTasks).await());
                exportTasks.clear();
            }
        }

        if (!exportTasks.isEmpty()) {
            results.addAll(ctx.allOf(exportTasks).await());
        }

        return results;
    }

    private static void commitCheckpoint(
            TaskOrchestrationContext ctx,
            EntityInstanceId jobEntityId,
            long scannedInstances,
            long exportedInstances,
            ExportCheckpoint checkpoint,
            List<ExportFailure> failures) {
        CommitCheckpointRequest request = new CommitCheckpointRequest();
        request.setScannedInstances(scannedInstances);
        request.setExportedInstances(exportedInstances);
        request.setCheckpoint(checkpoint);
        request.setFailures(failures);
        ctx.getEntities()
                .callEntity(jobEntityId, ExportJobTransitions.OP_COMMIT_CHECKPOINT, request, Void.class)
                .await();
    }

    private static void markAsCompleted(TaskOrchestrationContext ctx, EntityInstanceId jobEntityId) {
        ctx.getEntities()
                .callEntity(jobEntityId, ExportJobTransitions.OP_MARK_AS_COMPLETED, null, Void.class)
                .await();
    }

    private static void markAsFailed(
            TaskOrchestrationContext ctx, EntityInstanceId jobEntityId, String errorMessage) {
        ctx.getEntities()
                .callEntity(jobEntityId, ExportJobTransitions.OP_MARK_AS_FAILED, errorMessage, Void.class)
                .await();
    }

    private static String summarizeFailures(List<ExportFailure> failures) {
        if (failures == null || failures.isEmpty()) {
            return "No failure details available.";
        }
        String details = failures.stream()
                .limit(10)
                .map(f -> "InstanceId: " + f.getInstanceId() + ", Reason: " + f.getReason())
                .collect(Collectors.joining("; "));
        if (failures.size() > 10) {
            details += " ... and " + (failures.size() - 10) + " more failures";
        }
        return "Failure details: " + details;
    }

    private static final class BatchExportResult {
        private final boolean allSucceeded;
        private final int exportedCount;
        private final List<ExportFailure> failures;

        private BatchExportResult(boolean allSucceeded, int exportedCount, List<ExportFailure> failures) {
            this.allSucceeded = allSucceeded;
            this.exportedCount = exportedCount;
            this.failures = failures;
        }

        static BatchExportResult succeeded(int exportedCount) {
            return new BatchExportResult(true, exportedCount, null);
        }

        static BatchExportResult failed(int exportedCount, List<ExportFailure> failures) {
            return new BatchExportResult(false, exportedCount, failures);
        }
    }
}

/**
 * Input to the {@link ExportJobOrchestrator} identifying the job entity and the number of processed cycles
 * (used to bound work before {@code continueAsNew}).
 */
final class ExportJobRunRequest {

    private EntityInstanceId jobEntityId;
    private int processedCycles;

    /** Creates an empty {@code ExportJobRunRequest} (for deserialization). */
    public ExportJobRunRequest() {
    }

    /**
     * Creates an {@code ExportJobRunRequest}.
     *
     * @param jobEntityId     the export job entity ID
     * @param processedCycles the number of cycles already processed in this orchestration generation
     */
    public ExportJobRunRequest(EntityInstanceId jobEntityId, int processedCycles) {
        this.jobEntityId = jobEntityId;
        this.processedCycles = processedCycles;
    }

    /** @return the export job entity ID. */
    public EntityInstanceId getJobEntityId() {
        return this.jobEntityId;
    }

    /**
     * Sets the export job entity ID.
     *
     * @param jobEntityId the entity ID
     */
    public void setJobEntityId(EntityInstanceId jobEntityId) {
        this.jobEntityId = jobEntityId;
    }

    /** @return the number of cycles already processed in this orchestration generation. */
    public int getProcessedCycles() {
        return this.processedCycles;
    }

    /**
     * Sets the number of cycles already processed.
     *
     * @param processedCycles the processed cycle count
     */
    public void setProcessedCycles(int processedCycles) {
        this.processedCycles = processedCycles;
    }
}
