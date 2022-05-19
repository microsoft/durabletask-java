// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.time.Duration;

public final class RetryContext {
    private final TaskOrchestrationContext orchestrationContext;
    private final int lastAttemptNumber;
    private final FailureDetails lastFailure;
    private final Duration totalRetryTime;

    RetryContext(
            TaskOrchestrationContext orchestrationContext,
            int lastAttemptNumber,
            FailureDetails lastFailure,
            Duration totalRetryTime) {
        this.orchestrationContext = orchestrationContext;
        this.lastAttemptNumber = lastAttemptNumber;
        this.lastFailure = lastFailure;
        this.totalRetryTime = totalRetryTime;
    }

    public TaskOrchestrationContext getOrchestrationContext() {
        return this.orchestrationContext;
    }

    public FailureDetails getLastFailure() {
        return this.lastFailure;
    }

    public int getLastAttemptNumber() {
        return this.lastAttemptNumber;
    }

    public Duration getTotalRetryTime() {
        return this.totalRetryTime;
    }
}
