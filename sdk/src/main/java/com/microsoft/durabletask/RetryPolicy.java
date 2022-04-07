// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;

public class RetryPolicy {

    private final int maxNumberOfAttempts;
    private final Duration firstRetryInterval;
    private final double backoffCoefficient;
    private final Duration maxRetryInterval;
    private final Duration retryTimeout;

    private RetryPolicy(Builder builder) {
        this.maxNumberOfAttempts = builder.maxNumberOfAttempts;
        this.firstRetryInterval = builder.firstRetryInterval;
        this.backoffCoefficient = builder.backoffCoefficient;
        this.maxRetryInterval = Objects.requireNonNullElse(builder.maxRetryInterval, Duration.ZERO);
        this.retryTimeout = Objects.requireNonNullElse(builder.retryTimeout, Duration.ZERO);
    }

    public static Builder newBuilder(int maxNumberOfAttempts, Duration firstRetryInterval) {
        return new Builder(maxNumberOfAttempts, firstRetryInterval);
    }

    public int getMaxNumberOfAttempts() {
        return this.maxNumberOfAttempts;
    }

    public Duration getFirstRetryInterval() {
        return this.firstRetryInterval;
    }

    public double getBackoffCoefficient() {
        return this.backoffCoefficient;
    }

    public Duration getMaxRetryInterval() {
        return this.maxRetryInterval;
    }

    public Duration getRetryTimeout() {
        return this.retryTimeout;
     }

    public static class Builder {
        private int maxNumberOfAttempts;
        private Duration firstRetryInterval;
        private double backoffCoefficient;
        private Duration maxRetryInterval;
        private Duration retryTimeout;

        private Builder(int maxNumberOfAttempts, Duration firstRetryInterval) {
            this.setMaxNumberOfAttempts(maxNumberOfAttempts);
            this.setFirstRetryInterval(firstRetryInterval);
            this.setBackoffCoefficient(1.0);
        }

        public RetryPolicy build() {
            return new RetryPolicy(this);
        }

        public Builder setMaxNumberOfAttempts(int maxNumberOfAttempts) {
            if (maxNumberOfAttempts <= 0) {
                throw new IllegalArgumentException("The value for maxNumberOfAttempts must be greater than zero.");
            }
            this.maxNumberOfAttempts = maxNumberOfAttempts;
            return this;
        }

        public Builder setFirstRetryInterval(Duration firstRetryInterval) {
            if (firstRetryInterval == null) {
                throw new IllegalArgumentException("firstRetryInterval cannot be null.");
            }
            if (firstRetryInterval.isZero() || firstRetryInterval.isNegative()) {
                throw new IllegalArgumentException("The value for firstRetryInterval must be greater than zero.");
            }
            this.firstRetryInterval = firstRetryInterval;
            return this;
        }

        public Builder setBackoffCoefficient(double backoffCoefficient) {
            if (backoffCoefficient < 1.0) {
                throw new IllegalArgumentException("The value for backoffCoefficient must be greater or equal to 1.0.");
            }
            this.backoffCoefficient = backoffCoefficient;
            return this;
        }

        public Builder setMaxRetryInterval(@Nullable Duration maxRetryInterval) {
            if (maxRetryInterval != null && maxRetryInterval.compareTo(this.firstRetryInterval) < 0) {
                throw new IllegalArgumentException("The value for maxRetryInterval must be greater than or equal to the value for firstRetryInterval.");
            }
            this.maxRetryInterval = maxRetryInterval;
            return this;
        }

        public Builder setRetryTimeout(Duration retryTimeout) {
            if (retryTimeout != null && retryTimeout.compareTo(this.firstRetryInterval) < 0) {
                throw new IllegalArgumentException("The value for retryTimeout must be greater than or equal to the value for firstRetryInterval.");
            }
            this.retryTimeout = retryTimeout;
            return this;
        }
    }
}
