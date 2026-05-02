// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

/**
 * Lifecycle status of an export job.
 */
public enum ExportJobStatus {
    /**
     * Export job has been created but is not yet active.
     */
    PENDING,

    /**
     * Export job is active and running.
     */
    ACTIVE,

    /**
     * Export job failed.
     */
    FAILED,

    /**
     * Export job completed.
     */
    COMPLETED
}
