// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

/**
 * Constants used throughout the export history functionality.
 * <p>
 * Mirrors the .NET {@code Microsoft.DurableTask.ExportHistory.ExportHistoryConstants} source of truth.
 */
public final class ExportHistoryConstants {

    /**
     * The prefix used for generating export job orchestrator instance IDs.
     * Format: {@code "ExportJob-{jobId}"}.
     */
    public static final String ORCHESTRATOR_INSTANCE_ID_PREFIX = "ExportJob-";

    private ExportHistoryConstants() {
    }

    /**
     * Generates an orchestrator instance ID for the given export job ID.
     *
     * @param jobId the export job ID
     * @return the orchestrator instance ID
     */
    public static String getOrchestratorInstanceId(String jobId) {
        return ORCHESTRATOR_INSTANCE_ID_PREFIX + jobId;
    }
}
