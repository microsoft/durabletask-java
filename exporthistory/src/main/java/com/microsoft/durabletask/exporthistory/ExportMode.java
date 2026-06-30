// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

/**
 * Export job modes.
 * <p>
 * Mirrors the .NET {@code Microsoft.DurableTask.ExportHistory.ExportMode} source of truth.
 */
public enum ExportMode {
    /** Exports a fixed completion-time window and then completes. */
    BATCH,

    /** Tails terminal instances continuously until the job is stopped. */
    CONTINUOUS
}
