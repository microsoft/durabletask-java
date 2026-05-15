// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.constants;

import javax.annotation.Nonnull;

/**
 * Shared constants for the export history feature.
 */
public final class ExportHistoryConstants {

    private static final String ORCHESTRATOR_INSTANCE_ID_PREFIX = "export-job-";

    private ExportHistoryConstants() {
    }

    /**
     * Derives a deterministic orchestrator instance ID for a given export job.
     * This ensures only one orchestrator runs per job.
     *
     * @param jobId the export job ID
     * @return the orchestrator instance ID
     */
    @Nonnull
    public static String getOrchestratorInstanceId(@Nonnull String jobId) {
        return ORCHESTRATOR_INSTANCE_ID_PREFIX + jobId;
    }
}
