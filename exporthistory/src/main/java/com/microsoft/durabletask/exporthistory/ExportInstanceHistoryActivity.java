// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.TaskActivity;
import com.microsoft.durabletask.TaskActivityContext;
import com.microsoft.durabletask.history.HistoryEvent;

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
public final class ExportInstanceHistoryActivity implements TaskActivity {

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
            OrchestrationMetadata metadata = this.client.getInstanceMetadata(instanceId, true);
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
