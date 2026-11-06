// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

/**
 * Abstraction for storing and retrieving large payloads out-of-band.
 * <p>
 * Implementations upload payloads to an external store and return opaque reference tokens.
 * These tokens can be embedded in orchestration messages in place of the original payload data,
 * and later resolved back to the original payload.
 */
public abstract class PayloadStore {

    /**
     * Uploads a payload and returns an opaque reference token that can be embedded
     * in orchestration messages.
     *
     * @param payload the payload string to upload
     * @return an opaque reference token
     * @throws PayloadStorageException if the upload fails permanently
     */
    public abstract String upload(String payload);

    /**
     * Downloads the payload referenced by the given token.
     *
     * @param token the opaque reference token returned by {@link #upload(String)}
     * @return the original payload string
     * @throws PayloadStorageException if the download fails permanently
     */
    public abstract String download(String token);

    /**
     * Returns {@code true} if the specified value appears to be a token understood by this store.
     * <p>
     * Implementations should not throw for unknown tokens.
     *
     * @param value the value to check
     * @return {@code true} if the value is a token issued by this store; otherwise {@code false}
     */
    public abstract boolean isKnownPayloadToken(String value);

    /**
     * Gets an upper bound on the UTF-8 token size including protobuf field overhead,
     * used by workers to estimate post-externalization action sizes.
     * Custom stores with longer tokens should override this method.
     *
     * @return the estimated token wire size in bytes
     */
    public int getEstimatedTokenSizeBytes() {
        return 160;
    }
}
