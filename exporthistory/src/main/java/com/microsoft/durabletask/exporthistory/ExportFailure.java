// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import java.time.Instant;

/**
 * Failure of a specific instance export.
 */
final class ExportFailure {

    private String instanceId;
    private String reason;
    private int attemptCount;
    private Instant lastAttempt;

    /** Creates an empty {@code ExportFailure} (for deserialization). */
    public ExportFailure() {
    }

    /**
     * Creates an {@code ExportFailure}.
     *
     * @param instanceId   the instance ID that failed to export
     * @param reason       the failure reason
     * @param attemptCount the number of attempts made
     * @param lastAttempt  the timestamp of the last attempt
     */
    public ExportFailure(String instanceId, String reason, int attemptCount, Instant lastAttempt) {
        this.instanceId = instanceId;
        this.reason = reason;
        this.attemptCount = attemptCount;
        this.lastAttempt = lastAttempt;
    }

    /** @return the instance ID that failed to export. */
    public String getInstanceId() {
        return this.instanceId;
    }

    /**
     * Sets the instance ID that failed to export.
     *
     * @param instanceId the instance ID
     */
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    /** @return the failure reason. */
    public String getReason() {
        return this.reason;
    }

    /**
     * Sets the failure reason.
     *
     * @param reason the reason
     */
    public void setReason(String reason) {
        this.reason = reason;
    }

    /** @return the number of attempts made. */
    public int getAttemptCount() {
        return this.attemptCount;
    }

    /**
     * Sets the number of attempts made.
     *
     * @param attemptCount the attempt count
     */
    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    /** @return the timestamp of the last attempt. */
    public Instant getLastAttempt() {
        return this.lastAttempt;
    }

    /**
     * Sets the timestamp of the last attempt.
     *
     * @param lastAttempt the last attempt time
     */
    public void setLastAttempt(Instant lastAttempt) {
        this.lastAttempt = lastAttempt;
    }
}
