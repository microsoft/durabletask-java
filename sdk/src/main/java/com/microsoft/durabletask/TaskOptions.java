// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public class TaskOptions {
    private final RetryPolicy retryPolicy;
    private final RetryHandler retryHandler;

    private TaskOptions(Builder builder) {
        this.retryPolicy = builder.retryPolicy;
        this.retryHandler = builder.retryHandler;
    }

    public static TaskOptions fromRetryPolicy(RetryPolicy policy) {
        return newBuilder().setRetryStrategy(policy).build();
    }

    public static TaskOptions fromRetryHandler(RetryHandler handler) {
        return newBuilder().setRetryStrategy(handler).build();
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

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private RetryPolicy retryPolicy;
        private RetryHandler retryHandler;

        private Builder() {
        }

        public TaskOptions build() {
            return new TaskOptions(this);
        }

        public Builder setRetryStrategy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            this.retryHandler = null;
            return this;
        }

        public Builder setRetryStrategy(RetryHandler retryHandler) {
            this.retryHandler = retryHandler;
            this.retryPolicy = null;
            return this;
        }
    }
}
