// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for {@link ExportJobDescription} projection and {@link ExportFormat} value semantics.
 */
class ExportJobDescriptionTest {

    @Test
    void fromState_projectsAllFields() {
        Instant created = Instant.parse("2026-06-30T10:00:00Z");
        Instant modified = Instant.parse("2026-06-30T11:00:00Z");
        Instant checkpointTime = Instant.parse("2026-06-30T10:30:00Z");

        ExportJobConfiguration config = new ExportJobConfiguration();
        ExportCheckpoint checkpoint = new ExportCheckpoint("cursor-1");

        ExportJobState state = new ExportJobState();
        state.setStatus(ExportJobStatus.ACTIVE);
        state.setConfig(config);
        state.setCheckpoint(checkpoint);
        state.setCreatedAt(created);
        state.setLastModifiedAt(modified);
        state.setLastCheckpointTime(checkpointTime);
        state.setLastError("oops");
        state.setScannedInstances(10);
        state.setExportedInstances(7);
        state.setOrchestratorInstanceId("ExportJob-job-1");

        ExportJobDescription d = ExportJobDescription.fromState("job-1", state);

        assertEquals("job-1", d.getJobId());
        assertEquals(ExportJobStatus.ACTIVE, d.getStatus());
        assertSame(config, d.getConfig());
        assertSame(checkpoint, d.getCheckpoint());
        assertEquals(created, d.getCreatedAt());
        assertEquals(modified, d.getLastModifiedAt());
        assertEquals(checkpointTime, d.getLastCheckpointTime());
        assertEquals("oops", d.getLastError());
        assertEquals(10, d.getScannedInstances());
        assertEquals(7, d.getExportedInstances());
        assertEquals("ExportJob-job-1", d.getOrchestratorInstanceId());
    }

    @Test
    void exportFormat_defaultIsJsonl() {
        ExportFormat format = ExportFormat.getDefault();
        assertEquals(ExportFormatKind.JSONL, format.getKind());
        assertEquals(ExportFormat.DEFAULT_SCHEMA_VERSION, format.getSchemaVersion());
    }

    @Test
    void exportFormat_equalsAndHashCode() {
        ExportFormat a = new ExportFormat(ExportFormatKind.JSONL, "1.0");
        ExportFormat b = new ExportFormat(ExportFormatKind.JSONL, "1.0");
        ExportFormat c = new ExportFormat(ExportFormatKind.JSON, "1.0");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        org.junit.jupiter.api.Assertions.assertNotEquals(a, c);
    }

    @Test
    void exportHistoryConstants_orchestratorInstanceId() {
        assertEquals("ExportJob-job-9", ExportHistoryConstants.getOrchestratorInstanceId("job-9"));
    }
}
