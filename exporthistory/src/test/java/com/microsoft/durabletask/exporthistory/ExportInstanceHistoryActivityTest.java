// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.TaskActivityContext;
import com.microsoft.durabletask.history.GenericEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class ExportInstanceHistoryActivityTest {

    @Test
    void exportPreservesPrecisionInBodyAndBlobName() {
        Instant timestamp = Instant.parse("2026-09-15T12:34:56.1234567Z");
        DurableTaskClient client = mock(DurableTaskClient.class);
        BlobExportWriter writer = mock(BlobExportWriter.class);
        OrchestrationMetadata metadata = mock(OrchestrationMetadata.class);
        when(metadata.isInstanceFound()).thenReturn(true);
        when(metadata.isCompleted()).thenReturn(true);
        when(metadata.getLastUpdatedAt()).thenReturn(timestamp);
        when(client.getInstanceMetadata("instance-1", false)).thenReturn(metadata);
        when(client.getOrchestrationHistory("instance-1")).thenReturn(
                Collections.singletonList(new GenericEvent(1, timestamp, "payload")));
        ExportDestination destination = new ExportDestination("history");
        destination.setPrefix("exports/");
        ExportFormat format = new ExportFormat(ExportFormatKind.JSONL, "1.0");
        TaskActivityContext context = mock(TaskActivityContext.class);
        when(context.getInput(ExportRequest.class)).thenReturn(
                new ExportRequest("instance-1", destination, format));

        ExportResult result = (ExportResult) new ExportInstanceHistoryActivity(client, writer).run(context);

        assertTrue(result.isSuccess());
        verify(writer).upload(
                "history",
                "exports/8d8ce6e13a2dbef356275361521d0c3da44b809a81474169cd41732e82ecd2e4.jsonl.gz",
                "{\"eventType\":\"GenericEvent\",\"data\":\"payload\",\"eventId\":1,"
                        + "\"isPlayed\":false,\"timestamp\":\"2026-09-15T12:34:56.1234567Z\"}\n",
                format,
                "instance-1");
    }
}
