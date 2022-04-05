// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public class TaskOptions {
    private final RetryPolicy retryPolicy;

    TaskOptions(Builder builder) {
        this.retryPolicy = builder.retryPolicy;
    }

    public static TaskOptions fromRetryPolicy(RetryPolicy policy) {
        return newBuilder().setRetryPolicy(policy).build();
    }

    boolean hasRetryPolicy() {
        return this.retryPolicy != null;
    }

    public RetryPolicy getRetryPolicy() {
        return this.retryPolicy;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private RetryPolicy retryPolicy;

        private Builder() {
        }

        public TaskOptions build() {
            return new TaskOptions(this);
        }

        public Builder setRetryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }
    }
}
