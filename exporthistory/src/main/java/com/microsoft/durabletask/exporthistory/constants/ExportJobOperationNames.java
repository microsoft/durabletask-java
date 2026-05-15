// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.constants;

/**
 * Operation name constants for the {@code ExportJob} entity.
 * <p>
 * Java equivalent of .NET's {@code nameof(ExportJob.Create)} pattern.
 */
public final class ExportJobOperationNames {

    public static final String CREATE = "Create";
    public static final String GET = "Get";
    public static final String RUN = "Run";
    public static final String COMMIT_CHECKPOINT = "CommitCheckpoint";
    public static final String MARK_AS_COMPLETED = "MarkAsCompleted";
    public static final String MARK_AS_FAILED = "MarkAsFailed";
    public static final String DELETE = "Delete";

    private ExportJobOperationNames() {
    }
}
