// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.time.Duration;

/**
 * Context data that's provided to {@link RetryHandler} implementations.
 */
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

    /**
     * Gets the context of the current orchestration.
     * <p>
     * The orchestration context can be used in retry handlers to schedule timers (via the
     * {@link TaskOrchestrationContext#createTimer} methods) for implementing delays between retries. It can also be
     * used to implement time-based retry logic by using the {@link TaskOrchestrationContext#getCurrentInstant} method.
     *
     * @return the context of the parent orchestration
     */
    public TaskOrchestrationContext getOrchestrationContext() {
        return this.orchestrationContext;
    }

    /**
     * Gets the details of the previous task failure, including the exception type, message, and callstack.
     *
     * @return the details of the previous task failure
     */
    public FailureDetails getLastFailure() {
        return this.lastFailure;
    }

    /**
     * Gets the previous retry attempt number. This number starts at 1 and increments each time the retry handler
     * is invoked for a particular task failure.
     *
     * @return the previous retry attempt number
     */
    public int getLastAttemptNumber() {
        return this.lastAttemptNumber;
    }

    /**
     * Gets the total amount of time spent in a retry loop for the current task.
     *
     * @return the total amount of time spent in a retry loop for the current task
     */
    public Duration getTotalRetryTime() {
        return this.totalRetryTime;
    }
}
