// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Options that can be used to control the behavior of orchestrator and activity task execution.
 */
public class TaskOptions {
    private final RetryPolicy retryPolicy;
    private final RetryHandler retryHandler;

    private TaskOptions(RetryPolicy retryPolicy, RetryHandler retryHandler) {
        this.retryPolicy = retryPolicy;
        this.retryHandler = retryHandler;
    }

    /**
     * Creates a new {@code TaskOptions} object from a {@link RetryPolicy}.
     * @param retryPolicy the retry policy to use in the new {@code TaskOptions} object.
     */
    public TaskOptions(RetryPolicy retryPolicy) {
        this(retryPolicy, null);
    }

    /**
     * Creates a new {@code TaskOptions} object from a {@link RetryHandler}.
     * @param retryHandler the retry handler to use in the new {@code TaskOptions} object.
     */
    public TaskOptions(RetryHandler retryHandler) {
        this(null, retryHandler);
    }

    boolean hasRetryPolicy() {
        return this.retryPolicy != null;
    }

    /**
     * Gets the configured {@link RetryPolicy} value or {@code null} if none was configured.
     * @return the configured retry policy
     */
    public RetryPolicy getRetryPolicy() {
        return this.retryPolicy;
    }

    boolean hasRetryHandler() {
        return this.retryHandler != null;
    }

    /**
     * Gets the configured {@link RetryHandler} value or {@code null} if none was configured.
     * @return the configured retry handler.
     */
    public RetryHandler getRetryHandler() {
        return this.retryHandler;
    }
}
