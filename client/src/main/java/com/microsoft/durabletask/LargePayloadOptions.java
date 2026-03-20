// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Configuration options for large payload externalization.
 * <p>
 * This class defines the size thresholds that control when payloads are externalized
 * to a {@link PayloadStore}. It is a pure configuration class and does not hold a
 * reference to any {@link PayloadStore} implementation.
 * <p>
 * Use the {@link Builder} to create instances:
 * <pre>{@code
 * LargePayloadOptions options = new LargePayloadOptions.Builder()
 *     .setThresholdBytes(900_000)
 *     .setMaxExternalizedPayloadBytes(10 * 1024 * 1024)
 *     .build();
 * }</pre>
 *
 * @see PayloadStore
 */
public final class LargePayloadOptions {

    /**
     * Default externalization threshold in bytes (900,000 bytes, matching .NET SDK).
     */
    static final int DEFAULT_THRESHOLD_BYTES = 900_000;

    /**
     * Default maximum externalized payload size in bytes (10 MiB, matching .NET SDK).
     */
    static final int DEFAULT_MAX_EXTERNALIZED_PAYLOAD_BYTES = 10 * 1024 * 1024;

    private final int thresholdBytes;
    private final int maxExternalizedPayloadBytes;

    private LargePayloadOptions(Builder builder) {
        this.thresholdBytes = builder.thresholdBytes;
        this.maxExternalizedPayloadBytes = builder.maxExternalizedPayloadBytes;
    }

    /**
     * Gets the size threshold in bytes above which payloads will be externalized.
     * Payloads at or below this size are sent inline. The comparison uses UTF-8 byte length.
     *
     * @return the externalization threshold in bytes
     */
    public int getThresholdBytes() {
        return this.thresholdBytes;
    }

    /**
     * Gets the maximum payload size in bytes that can be externalized.
     * Payloads exceeding this size will cause an error to be thrown.
     *
     * @return the maximum externalized payload size in bytes
     */
    public int getMaxExternalizedPayloadBytes() {
        return this.maxExternalizedPayloadBytes;
    }

    /**
     * Builder for constructing {@link LargePayloadOptions} instances.
     */
    public static final class Builder {
        private int thresholdBytes = DEFAULT_THRESHOLD_BYTES;
        private int maxExternalizedPayloadBytes = DEFAULT_MAX_EXTERNALIZED_PAYLOAD_BYTES;

        /**
         * Sets the size threshold in bytes above which payloads will be externalized.
         * Must not exceed 1 MiB (1,048,576 bytes).
         *
         * @param thresholdBytes the externalization threshold in bytes
         * @return this builder
         * @throws IllegalArgumentException if thresholdBytes is negative or exceeds 1 MiB
         */
        public Builder setThresholdBytes(int thresholdBytes) {
            if (thresholdBytes < 0) {
                throw new IllegalArgumentException("thresholdBytes must not be negative");
            }
            if (thresholdBytes > 1_048_576) {
                throw new IllegalArgumentException("thresholdBytes must not exceed 1 MiB (1,048,576 bytes)");
            }
            this.thresholdBytes = thresholdBytes;
            return this;
        }

        /**
         * Sets the maximum payload size in bytes that can be externalized.
         * Payloads exceeding this size will cause an error.
         *
         * @param maxExternalizedPayloadBytes the maximum externalized payload size in bytes
         * @return this builder
         * @throws IllegalArgumentException if maxExternalizedPayloadBytes is not positive
         */
        public Builder setMaxExternalizedPayloadBytes(int maxExternalizedPayloadBytes) {
            if (maxExternalizedPayloadBytes <= 0) {
                throw new IllegalArgumentException("maxExternalizedPayloadBytes must be positive");
            }
            this.maxExternalizedPayloadBytes = maxExternalizedPayloadBytes;
            return this;
        }

        /**
         * Builds a new {@link LargePayloadOptions} instance from the current builder settings.
         *
         * @return a new {@link LargePayloadOptions} instance
         * @throws IllegalStateException if thresholdBytes is not less than maxExternalizedPayloadBytes
         */
        public LargePayloadOptions build() {
            if (this.thresholdBytes >= this.maxExternalizedPayloadBytes) {
                throw new IllegalStateException(
                    "thresholdBytes (" + this.thresholdBytes +
                    ") must be less than maxExternalizedPayloadBytes (" + this.maxExternalizedPayloadBytes + ")");
            }
            return new LargePayloadOptions(this);
        }
    }
}
