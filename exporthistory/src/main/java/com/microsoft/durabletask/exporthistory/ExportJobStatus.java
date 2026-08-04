// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

/**
 * Represents the current status of an export history job.
 */
public enum ExportJobStatus {
    /** The export history job has been created but is not yet active. */
    PENDING,

    /** The export history job is active and running. */
    ACTIVE,

    /** The export history job failed. */
    FAILED,

    /** The export history job completed. */
    COMPLETED
}
