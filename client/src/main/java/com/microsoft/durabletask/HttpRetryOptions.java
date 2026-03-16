// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Defines retry policies for handling failures when making durable HTTP requests via
 * {@link DurableHttp#callHttp}.
 * <p>
 * Retry options define how the Durable Functions host should retry failed HTTP calls. Failures
 * include non-successful HTTP status codes, timeouts, or exceptions from the HTTP client.
 * <p>
 * Example usage:
 * <pre>{@code
 * HttpRetryOptions retryOptions = new HttpRetryOptions(Duration.ofSeconds(5), 3);
 * retryOptions.setBackoffCoefficient(2.0);
 * retryOptions.setMaxRetryInterval(Duration.ofMinutes(5));
 * retryOptions.setStatusCodesToRetry(Arrays.asList(500, 502, 503));
 *
 * DurableHttpRequest request = new DurableHttpRequest(
 *     "GET", new URI("https://example.com/api"),
 *     null, null, null, true,
 *     Duration.ofMinutes(10), retryOptions);
 * }</pre>
 *
 * @see DurableHttpRequest
 * @see DurableHttp
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HttpRetryOptions {

    private static final Duration DEFAULT_MAX_RETRY_INTERVAL = Duration.ofDays(6);
    private static final double DEFAULT_BACKOFF_COEFFICIENT = 1.0;

    private Duration firstRetryInterval;
    private int maxNumberOfAttempts;
    private Duration maxRetryInterval;
    private double backoffCoefficient;
    private Duration retryTimeout;
    private List<Integer> statusCodesToRetry;

    /**
     * Creates a new {@code HttpRetryOptions} with the specified first retry interval and max attempts.
     *
     * @param firstRetryInterval the duration to wait before the first retry attempt
     * @param maxNumberOfAttempts the maximum number of retry attempts
     * @throws IllegalArgumentException if firstRetryInterval is null or non-positive,
     *                                  or if maxNumberOfAttempts is less than 1
     */
    public HttpRetryOptions(Duration firstRetryInterval, int maxNumberOfAttempts) {
        if (firstRetryInterval == null) {
            throw new IllegalArgumentException("firstRetryInterval must not be null");
        }
        if (firstRetryInterval.isNegative() || firstRetryInterval.isZero()) {
            throw new IllegalArgumentException("firstRetryInterval must be greater than zero");
        }
        if (maxNumberOfAttempts < 1) {
            throw new IllegalArgumentException("maxNumberOfAttempts must be at least 1");
        }
        this.firstRetryInterval = firstRetryInterval;
        this.maxNumberOfAttempts = maxNumberOfAttempts;
        this.maxRetryInterval = DEFAULT_MAX_RETRY_INTERVAL;
        this.backoffCoefficient = DEFAULT_BACKOFF_COEFFICIENT;
        this.statusCodesToRetry = new ArrayList<>();
    }

    /**
     * Jackson deserialization constructor. Takes TimeSpan-formatted strings for Duration fields.
     */
    @JsonCreator
    HttpRetryOptions(
            @JsonProperty("firstRetryInterval") @Nullable String firstRetryIntervalStr,
            @JsonProperty("maxNumberOfAttempts") @Nullable Integer maxNumberOfAttempts,
            @JsonProperty("maxRetryInterval") @Nullable String maxRetryIntervalStr,
            @JsonProperty("backoffCoefficient") @Nullable Double backoffCoefficient,
            @JsonProperty("retryTimeout") @Nullable String retryTimeoutStr,
            @JsonProperty("statusCodesToRetry") @Nullable List<Integer> statusCodesToRetry) {
        this.firstRetryInterval = firstRetryIntervalStr != null
                ? TimeSpanHelper.parse(firstRetryIntervalStr) : null;
        this.maxNumberOfAttempts = maxNumberOfAttempts != null ? maxNumberOfAttempts : 0;
        this.maxRetryInterval = maxRetryIntervalStr != null
                ? TimeSpanHelper.parse(maxRetryIntervalStr) : DEFAULT_MAX_RETRY_INTERVAL;
        this.backoffCoefficient = backoffCoefficient != null ? backoffCoefficient : DEFAULT_BACKOFF_COEFFICIENT;
        this.retryTimeout = retryTimeoutStr != null ? TimeSpanHelper.parse(retryTimeoutStr) : null;
        this.statusCodesToRetry = statusCodesToRetry != null
                ? new ArrayList<>(statusCodesToRetry) : new ArrayList<>();
    }

    // ---- Getters (public API uses Duration) ----

    /**
     * Gets the first retry interval.
     *
     * @return the duration to wait before the first retry
     */
    @JsonIgnore
    public Duration getFirstRetryInterval() {
        return this.firstRetryInterval;
    }

    /**
     * Gets the maximum number of retry attempts.
     *
     * @return the maximum number of attempts
     */
    @JsonProperty("maxNumberOfAttempts")
    public int getMaxNumberOfAttempts() {
        return this.maxNumberOfAttempts;
    }

    /**
     * Gets the maximum retry interval.
     *
     * @return the maximum duration between retries, defaults to 6 days
     */
    @JsonIgnore
    public Duration getMaxRetryInterval() {
        return this.maxRetryInterval;
    }

    /**
     * Gets the backoff coefficient.
     *
     * @return the backoff coefficient used to determine the rate of increase of backoff, defaults to 1.0
     */
    @JsonProperty("backoffCoefficient")
    public double getBackoffCoefficient() {
        return this.backoffCoefficient;
    }

    /**
     * Gets the retry timeout.
     *
     * @return the overall timeout for retries, or {@code null} if no timeout is set
     */
    @JsonIgnore
    @Nullable
    public Duration getRetryTimeout() {
        return this.retryTimeout;
    }

    /**
     * Gets the list of HTTP status codes that should trigger a retry.
     * <p>
     * If the list is empty, all 4xx and 5xx status codes will be retried.
     *
     * @return an unmodifiable list of HTTP status codes to retry
     */
    @JsonProperty("statusCodesToRetry")
    public List<Integer> getStatusCodesToRetry() {
        return Collections.unmodifiableList(this.statusCodesToRetry);
    }

    // ---- Setters ----

    /**
     * Sets the maximum retry interval.
     *
     * @param maxRetryInterval the maximum duration between retries
     */
    public void setMaxRetryInterval(Duration maxRetryInterval) {
        this.maxRetryInterval = maxRetryInterval;
    }

    /**
     * Sets the backoff coefficient.
     *
     * @param backoffCoefficient the backoff coefficient (must be &gt;= 1.0)
     * @throws IllegalArgumentException if backoffCoefficient is less than 1.0, NaN, or infinite
     */
    public void setBackoffCoefficient(double backoffCoefficient) {
        if (Double.isNaN(backoffCoefficient) || Double.isInfinite(backoffCoefficient)) {
            throw new IllegalArgumentException("backoffCoefficient must be a finite number");
        }
        if (backoffCoefficient < 1.0) {
            throw new IllegalArgumentException("backoffCoefficient must be >= 1.0");
        }
        this.backoffCoefficient = backoffCoefficient;
    }

    /**
     * Sets the retry timeout.
     *
     * @param retryTimeout the overall timeout for retries, or {@code null} for no timeout
     */
    public void setRetryTimeout(@Nullable Duration retryTimeout) {
        this.retryTimeout = retryTimeout;
    }

    /**
     * Sets the list of HTTP status codes that should trigger a retry.
     *
     * @param statusCodesToRetry the status codes to retry, or {@code null} to retry all 4xx/5xx
     */
    public void setStatusCodesToRetry(@Nullable List<Integer> statusCodesToRetry) {
        this.statusCodesToRetry = statusCodesToRetry != null
                ? new ArrayList<>(statusCodesToRetry) : new ArrayList<>();
    }

    // ---- JSON serialization helpers (TimeSpan format for .NET compatibility) ----

    @JsonGetter("firstRetryInterval")
    @Nullable
    String getFirstRetryIntervalString() {
        return this.firstRetryInterval != null ? TimeSpanHelper.format(this.firstRetryInterval) : null;
    }

    @JsonGetter("maxRetryInterval")
    @Nullable
    String getMaxRetryIntervalString() {
        return this.maxRetryInterval != null ? TimeSpanHelper.format(this.maxRetryInterval) : null;
    }

    @JsonGetter("retryTimeout")
    @Nullable
    String getRetryTimeoutString() {
        return this.retryTimeout != null ? TimeSpanHelper.format(this.retryTimeout) : null;
    }
}
