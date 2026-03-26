// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Interface for externalizing and resolving large payloads to/from an external store.
 * <p>
 * Implementations of this interface handle uploading large payloads to external storage
 * (e.g., Azure Blob Storage) and returning opaque token references that can be resolved
 * back to the original data. This enables the Durable Task framework to handle payloads
 * that exceed gRPC message size limits.
 * <p>
 * The store implementation is solely responsible for generating blob names/keys and
 * defining the token format. The core framework treats tokens as opaque strings and
 * delegates token recognition to {@link #isKnownPayloadToken(String)}.
 * <p>
 * <b>Payload retention:</b> This interface does not define a deletion mechanism.
 * Externalized payloads persist until removed by external means. When using Azure
 * Blob Storage, configure
 * <a href="https://learn.microsoft.com/azure/storage/blobs/lifecycle-management-overview">
 * lifecycle management policies</a> to automatically expire old payloads.
 *
 * @see LargePayloadOptions
 */
public interface PayloadStore {
    /**
     * Uploads a payload string to external storage and returns an opaque token reference.
     *
     * @param payload the payload data to upload
     * @return an opaque token string that can be used to retrieve the payload via {@link #download(String)}
     */
    String upload(String payload);

    /**
     * Downloads a payload from external storage using the given token reference.
     *
     * @param token the opaque token returned by a previous {@link #upload(String)} call
     * @return the original payload string
     */
    String download(String token);

    /**
     * Determines whether the given value is a token that was produced by this store.
     * <p>
     * This method is used by the framework to distinguish between regular payload data
     * and externalized payload references. Only values recognized as tokens will be
     * resolved via {@link #download(String)}.
     *
     * @param value the string value to check
     * @return {@code true} if the value is a known payload token from this store; {@code false} otherwise
     */
    boolean isKnownPayloadToken(String value);
}
