// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;

public class RetryPolicy {

    private int maxNumberOfAttempts;
    private Duration firstRetryInterval;
    private double backoffCoefficient;
    private Duration maxRetryInterval;
    private Duration retryTimeout;

    public RetryPolicy() {
    }

    public RetryPolicy setMaxNumberOfAttempts(int maxNumberOfAttempts) {
        if (maxNumberOfAttempts <= 0) {
            throw new IllegalArgumentException("The value for maxNumberOfAttempts must be greater than zero.");
        }
        this.maxNumberOfAttempts = maxNumberOfAttempts;
        return this;
    }

    public RetryPolicy setFirstRetryInterval(Duration firstRetryInterval) {
        if (firstRetryInterval == null) {
            throw new IllegalArgumentException("firstRetryInterval cannot be null.");
        }
        if (firstRetryInterval.isZero() || firstRetryInterval.isNegative()) {
            throw new IllegalArgumentException("The value for firstRetryInterval must be greater than zero.");
        }
        this.firstRetryInterval = firstRetryInterval;
        return this;
    }

    public RetryPolicy setBackoffCoefficient(double backoffCoefficient) {
        if (backoffCoefficient < 1.0) {
            throw new IllegalArgumentException("The value for backoffCoefficient must be greater or equal to 1.0.");
        }
        this.backoffCoefficient = backoffCoefficient;
        return this;
    }

    public RetryPolicy setMaxRetryInterval(@Nullable Duration maxRetryInterval) {
        if (maxRetryInterval != null && maxRetryInterval.compareTo(this.firstRetryInterval) < 0) {
            throw new IllegalArgumentException("The value for maxRetryInterval must be greater than or equal to the value for firstRetryInterval.");
        }
        this.maxRetryInterval = maxRetryInterval;
        return this;
    }

    public RetryPolicy setRetryTimeout(Duration retryTimeout) {
        if (retryTimeout != null && retryTimeout.compareTo(this.firstRetryInterval) < 0) {
            throw new IllegalArgumentException("The value for retryTimeout must be greater than or equal to the value for firstRetryInterval.");
        }
        this.retryTimeout = retryTimeout;
        return this;
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
}
