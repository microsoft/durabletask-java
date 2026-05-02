// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nonnull;
import java.time.Instant;

/**
 * Records a failed export attempt for a single orchestration instance.
 */
public final class ExportFailure {

    private String instanceId;
    private String reason;
    private int attemptCount;
    private Instant lastAttempt;

    public ExportFailure() {
    }

    public ExportFailure(
            @Nonnull String instanceId,
            @Nonnull String reason,
            int attemptCount,
            @Nonnull Instant lastAttempt) {
        this.instanceId = instanceId;
        this.reason = reason;
        this.attemptCount = attemptCount;
        this.lastAttempt = lastAttempt;
    }

    @Nonnull
    public String getInstanceId() {
        return this.instanceId;
    }

    @Nonnull
    public String getReason() {
        return this.reason;
    }

    public int getAttemptCount() {
        return this.attemptCount;
    }

    @Nonnull
    public Instant getLastAttempt() {
        return this.lastAttempt;
    }
}
