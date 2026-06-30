// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import javax.annotation.Nullable;

/**
 * Output of {@link ExportInstanceHistoryActivity}: whether a single instance's history export succeeded, and the
 * blob name written (or the error on failure).
 */
public final class ExportResult {

    private String instanceId;
    private boolean success;
    private String error;
    private String blobName;

    /** Creates an empty {@code ExportResult} (for deserialization). */
    public ExportResult() {
    }

    /**
     * Creates a successful {@code ExportResult}.
     *
     * @param instanceId the exported instance ID
     * @param blobName   the blob name written
     * @return a success result
     */
    public static ExportResult success(String instanceId, String blobName) {
        ExportResult result = new ExportResult();
        result.instanceId = instanceId;
        result.success = true;
        result.blobName = blobName;
        return result;
    }

    /**
     * Creates a failed {@code ExportResult}.
     *
     * @param instanceId the instance ID that failed
     * @param error      the error message
     * @return a failure result
     */
    public static ExportResult failure(String instanceId, String error) {
        ExportResult result = new ExportResult();
        result.instanceId = instanceId;
        result.success = false;
        result.error = error;
        return result;
    }

    /** @return the instance ID. */
    public String getInstanceId() {
        return this.instanceId;
    }

    /**
     * Sets the instance ID.
     *
     * @param instanceId the instance ID
     */
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    /** @return {@code true} if the export succeeded. */
    public boolean isSuccess() {
        return this.success;
    }

    /**
     * Sets whether the export succeeded.
     *
     * @param success {@code true} if successful
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }

    /** @return the error message, or {@code null} on success. */
    @Nullable
    public String getError() {
        return this.error;
    }

    /**
     * Sets the error message.
     *
     * @param error the error message, or {@code null}
     */
    public void setError(@Nullable String error) {
        this.error = error;
    }

    /** @return the blob name written, or {@code null} on failure. */
    @Nullable
    public String getBlobName() {
        return this.blobName;
    }

    /**
     * Sets the blob name written.
     *
     * @param blobName the blob name, or {@code null}
     */
    public void setBlobName(@Nullable String blobName) {
        this.blobName = blobName;
    }
}
