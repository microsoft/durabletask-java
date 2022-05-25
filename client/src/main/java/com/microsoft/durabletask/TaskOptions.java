// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public final class TaskOptions {
    private final RetryPolicy retryPolicy;
    private final RetryHandler retryHandler;

    private TaskOptions(RetryPolicy retryPolicy, RetryHandler retryHandler) {
        this.retryPolicy = retryPolicy;
        this.retryHandler = retryHandler;
    }

    public TaskOptions(RetryPolicy retryPolicy) {
        this(retryPolicy, null);
    }

    public TaskOptions(RetryHandler retryHandler) {
        this(null, retryHandler);
    }

    boolean hasRetryPolicy() {
        return this.retryPolicy != null;
    }

    public RetryPolicy getRetryPolicy() {
        return this.retryPolicy;
    }

    boolean hasRetryHandler() {
        return this.retryHandler != null;
    }

    public RetryHandler getRetryHandler() {
        return this.retryHandler;
    }
}
