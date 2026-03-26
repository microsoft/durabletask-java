// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.nio.charset.StandardCharsets;

/**
 * Internal utility for externalizing and resolving payloads using a {@link PayloadStore}.
 * <p>
 * This class encapsulates the threshold-checking and store-delegation logic that is shared
 * across the worker, client, and orchestration runner code paths.
 */
final class PayloadHelper {

    private final PayloadStore store;
    private final LargePayloadOptions options;

    /**
     * Creates a new PayloadHelper.
     *
     * @param store   the payload store to use for upload/download operations
     * @param options the large payload configuration options
     */
    PayloadHelper(PayloadStore store, LargePayloadOptions options) {
        if (store == null) {
            throw new IllegalArgumentException("store must not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("options must not be null");
        }
        this.store = store;
        this.options = options;
    }

    /**
     * Externalizes the given value if it exceeds the configured threshold.
     * <p>
     * The check order matches .NET: (1) null/empty guard, (2) below-threshold guard,
     * (3) above-max-cap rejection, (4) upload.
     *
     * @param value the payload string to potentially externalize
     * @return the original value if below threshold, or an opaque token if externalized
     * @throws IllegalArgumentException if the payload exceeds the maximum externalized payload size
     */
    String maybeExternalize(String value) {
        // (1) null/empty guard
        if (value == null || value.isEmpty()) {
            return value;
        }

        // Fast path: if char count is below threshold, byte count is too
        // (each Java char encodes to 1-3 UTF-8 bytes, so length() <= UTF-8 byte length)
        if (value.length() <= this.options.getThresholdBytes()) {
            return value;
        }

        int byteSize = value.getBytes(StandardCharsets.UTF_8).length;

        // (2) below-threshold guard
        if (byteSize <= this.options.getThresholdBytes()) {
            return value;
        }

        // (3) above-max-cap rejection
        if (byteSize > this.options.getMaxExternalizedPayloadBytes()) {
            throw new PayloadTooLargeException(String.format(
                "Payload size %d KB exceeds maximum of %d KB. " +
                "Reduce the payload size or increase maxExternalizedPayloadBytes.",
                byteSize / 1024,
                this.options.getMaxExternalizedPayloadBytes() / 1024));
        }

        // (4) upload
        return this.store.upload(value);
    }

    /**
     * Resolves the given value if it is a known payload token.
     *
     * @param value the string to potentially resolve
     * @return the resolved payload data if the value was a token, or the original value otherwise
     */
    String maybeResolve(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }

        if (!this.store.isKnownPayloadToken(value)) {
            return value;
        }

        String resolved = this.store.download(value);
        if (resolved == null) {
            throw new IllegalStateException(
                "PayloadStore.download() returned null for token: " + value);
        }
        return resolved;
    }
}
