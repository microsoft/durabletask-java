// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.constants;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ExportHistoryConstants}.
 */
class ExportHistoryConstantsTest {

    @Test
    void getOrchestratorInstanceId_includesJobId() {
        String instanceId = ExportHistoryConstants.getOrchestratorInstanceId("job-123");
        assertTrue(instanceId.contains("job-123"));
    }

    @Test
    void getOrchestratorInstanceId_deterministic() {
        String id1 = ExportHistoryConstants.getOrchestratorInstanceId("job-abc");
        String id2 = ExportHistoryConstants.getOrchestratorInstanceId("job-abc");
        assertEquals(id1, id2);
    }

    @Test
    void getOrchestratorInstanceId_differentJobIds_produceDifferentIds() {
        String id1 = ExportHistoryConstants.getOrchestratorInstanceId("job-1");
        String id2 = ExportHistoryConstants.getOrchestratorInstanceId("job-2");
        assertNotEquals(id1, id2);
    }
}
