// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.TaskActivity;
import com.microsoft.durabletask.TaskActivityContext;
import com.microsoft.durabletask.history.HistoryEvent;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Activity that exports a single orchestration instance's history to the configured blob destination.
 * <p>
 * It reads the instance metadata to confirm a terminal state and obtain the completion timestamp, streams the full
 * history via {@code getOrchestrationHistory}, serializes it (gzipped JSONL by default), and uploads it to a blob
 * named from a hash of the completion timestamp and instance ID.
 */
final class ExportInstanceHistoryActivity implements TaskActivity {

    /** The registered activity name. */
    public static final String NAME = "ExportInstanceHistoryActivity";

    private static final Logger LOGGER = Logger.getLogger(ExportInstanceHistoryActivity.class.getName());

    private final DurableTaskClient client;
    private final BlobExportWriter writer;

    /**
     * Creates an {@code ExportInstanceHistoryActivity}.
     *
     * @param client the Durable Task client used to read metadata and history
     * @param writer the blob writer used to upload serialized history
     */
    public ExportInstanceHistoryActivity(DurableTaskClient client, BlobExportWriter writer) {
        this.client = client;
        this.writer = writer;
    }

    @Override
    public Object run(TaskActivityContext ctx) {
        ExportRequest input = ctx.getInput(ExportRequest.class);
        String instanceId = input.getInstanceId();

        try {
            OrchestrationMetadata metadata = this.client.getInstanceMetadata(instanceId, false);
            if (metadata == null || !metadata.isInstanceFound()) {
                return ExportResult.failure(instanceId, "Instance " + instanceId + " not found");
            }
            if (!metadata.isCompleted()) {
                return ExportResult.failure(instanceId, "Instance " + instanceId + " is not in a completed state");
            }

            Instant completedTimestamp = metadata.getLastUpdatedAt();
            List<HistoryEvent> historyEvents = this.client.getOrchestrationHistory(instanceId);

            String blobFileName = ExportBlobNaming.blobFileName(completedTimestamp, instanceId, input.getFormat());
            String blobPath = ExportBlobNaming.blobPath(input.getDestination().getPrefix(), blobFileName);

            String content = HistoryEventSerializer.serialize(historyEvents, input.getFormat());
            this.writer.upload(
                    input.getDestination().getContainer(),
                    blobPath,
                    content,
                    input.getFormat(),
                    instanceId);

            LOGGER.log(Level.FINE, "Exported instance {0} ({1} events) to blob {2}",
                    new Object[] {instanceId, historyEvents.size(), blobPath});
            return ExportResult.success(instanceId, blobPath);
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "Failed to export instance " + instanceId, ex);
            return ExportResult.failure(instanceId, ex.getMessage());
        }
    }
}

/**
 * Input to {@link ExportInstanceHistoryActivity}: the instance to export plus the destination and format.
 */
final class ExportRequest {

    private String instanceId;
    private ExportDestination destination;
    private ExportFormat format;

    /** Creates an empty {@code ExportRequest} (for deserialization). */
    public ExportRequest() {
    }

    /**
     * Creates an {@code ExportRequest}.
     *
     * @param instanceId  the instance ID to export
     * @param destination the export destination
     * @param format      the export format
     */
    public ExportRequest(String instanceId, ExportDestination destination, ExportFormat format) {
        this.instanceId = instanceId;
        this.destination = destination;
        this.format = format;
    }

    /** @return the instance ID to export. */
    public String getInstanceId() {
        return this.instanceId;
    }

    /**
     * Sets the instance ID to export.
     *
     * @param instanceId the instance ID
     */
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    /** @return the export destination. */
    public ExportDestination getDestination() {
        return this.destination;
    }

    /**
     * Sets the export destination.
     *
     * @param destination the destination
     */
    public void setDestination(ExportDestination destination) {
        this.destination = destination;
    }

    /** @return the export format. */
    public ExportFormat getFormat() {
        return this.format;
    }

    /**
     * Sets the export format.
     *
     * @param format the format
     */
    public void setFormat(ExportFormat format) {
        this.format = format;
    }
}

/**
 * Output of {@link ExportInstanceHistoryActivity}: whether a single instance's history export succeeded, and the
 * blob name written (or the error on failure).
 */
final class ExportResult {

    private String instanceId;
    private boolean success;
    private String error;
    private String blobName;

    /** Creates an empty {@code ExportResult} (for deserialization). */
    public ExportResult() {
    }

    /**
     * Creates a successful {@code ExportResult}.
     *
     * @param instanceId the exported instance ID
     * @param blobName   the blob name written
     * @return a success result
     */
    public static ExportResult success(String instanceId, String blobName) {
        ExportResult result = new ExportResult();
        result.instanceId = instanceId;
        result.success = true;
        result.blobName = blobName;
        return result;
    }

    /**
     * Creates a failed {@code ExportResult}.
     *
     * @param instanceId the instance ID that failed
     * @param error      the error message
     * @return a failure result
     */
    public static ExportResult failure(String instanceId, String error) {
        ExportResult result = new ExportResult();
        result.instanceId = instanceId;
        result.success = false;
        result.error = error;
        return result;
    }

    /** @return the instance ID. */
    public String getInstanceId() {
        return this.instanceId;
    }

    /**
     * Sets the instance ID.
     *
     * @param instanceId the instance ID
     */
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    /** @return {@code true} if the export succeeded. */
    public boolean isSuccess() {
        return this.success;
    }

    /**
     * Sets whether the export succeeded.
     *
     * @param success {@code true} if successful
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }

    /** @return the error message, or {@code null} on success. */
    @Nullable
    public String getError() {
        return this.error;
    }

    /**
     * Sets the error message.
     *
     * @param error the error message, or {@code null}
     */
    public void setError(@Nullable String error) {
        this.error = error;
    }

    /** @return the blob name written, or {@code null} on failure. */
    @Nullable
    public String getBlobName() {
        return this.blobName;
    }

    /**
     * Sets the blob name written.
     *
     * @param blobName the blob name, or {@code null}
     */
    public void setBlobName(@Nullable String blobName) {
        this.blobName = blobName;
    }
}
