// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Result of exporting a single orchestration instance's history.
 */
public final class ExportResult {

    private String instanceId;
    private boolean success;
    private String error;

    // Default constructor for Jackson deserialization
    public ExportResult() {
    }

    public ExportResult(@Nonnull String instanceId, boolean success, @Nullable String error) {
        this.instanceId = instanceId;
        this.success = success;
        this.error = error;
    }

    @Nonnull
    public String getInstanceId() { return this.instanceId; }
    public void setInstanceId(@Nonnull String instanceId) { this.instanceId = instanceId; }

    public boolean isSuccess() { return this.success; }
    public void setSuccess(boolean success) { this.success = success; }

    @Nullable
    public String getError() { return this.error; }
    public void setError(@Nullable String error) { this.error = error; }
}
