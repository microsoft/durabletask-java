// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public class TaskOptions {
    private final RetryPolicy retryPolicy;
    private final RetryHandler retryHandler;

    TaskOptions(Builder builder) {
        this.retryPolicy = builder.retryPolicy;
        this.retryHandler = builder.retryHandler;
    }

    public static TaskOptions fromRetryPolicy(RetryPolicy policy) {
        return newBuilder().setRetryPolicy(policy).build();
    }

    public static TaskOptions fromRetryHandler(RetryHandler handler) {
        return newBuilder().setRetryHandler(handler).build();
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

        public Builder setRetryPolicy(RetryPolicy retryPolicy) {
            if (this.retryHandler != null) {
                throw new IllegalStateException("You can configure a retry policy or a retry handler, but not both.");
            }

            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder setRetryHandler(RetryHandler retryHandler) {
            if (this.retryPolicy != null) {
                throw new IllegalStateException("You can configure a retry policy or a retry handler, but not both.");
            }

            this.retryHandler = retryHandler;
            return this;
        }
    }
}
