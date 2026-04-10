// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobDownloadToFileOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Azure Blob Storage implementation of {@link PayloadStore}.
 * <p>
 * Stores payloads as blobs and returns opaque tokens in the form {@code blob:v1:<container>:<blobName>}.
 * Supports optional gzip compression. The blob container is created automatically on first upload.
 */
public final class BlobPayloadStore extends PayloadStore {

    static final String TOKEN_PREFIX = "blob:v1:";
    private static final String CONTENT_ENCODING_GZIP = "gzip";

    private final BlobContainerClient containerClient;
    private final LargePayloadStorageOptions options;

    /**
     * Creates a new {@code BlobPayloadStore} from the given options.
     *
     * @param options the storage options
     * @throws IllegalArgumentException if neither connection string nor account URI/credential are provided
     */
    public BlobPayloadStore(LargePayloadStorageOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null.");
        }

        String containerName = options.getContainerName();
        if (containerName == null || containerName.isEmpty()) {
            throw new IllegalArgumentException("Container name must not be null or empty.");
        }

        boolean hasConnectionString = options.getConnectionString() != null
                && !options.getConnectionString().isEmpty();
        boolean hasIdentityAuth = options.getAccountUri() != null && options.getCredential() != null;

        if (!hasConnectionString && !hasIdentityAuth) {
            throw new IllegalArgumentException(
                "Either ConnectionString or AccountUri and Credential must be provided.");
        }

        // Retry policy: exponential (8 retries, 250ms base, 10s max, 2min network timeout)
        // Matches the .NET BlobPayloadStore retry configuration.
        RequestRetryOptions retryOptions = new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            8,           // maxTries
            120,         // tryTimeoutInSeconds (2 min network timeout)
            250L,        // retryDelayInMs (250ms base)
            10_000L,     // maxRetryDelayInMs (10s max)
            null);       // secondaryHost

        BlobServiceClient serviceClient;
        if (hasIdentityAuth) {
            serviceClient = new BlobServiceClientBuilder()
                .endpoint(options.getAccountUri().toString())
                .credential(options.getCredential())
                .retryOptions(retryOptions)
                .buildClient();
        } else {
            serviceClient = new BlobServiceClientBuilder()
                .connectionString(options.getConnectionString())
                .retryOptions(retryOptions)
                .buildClient();
        }

        this.containerClient = serviceClient.getBlobContainerClient(containerName);
        this.options = options;
    }

    /**
     * Package-private constructor for testing with an injected {@link BlobContainerClient}.
     */
    BlobPayloadStore(BlobContainerClient containerClient, LargePayloadStorageOptions options) {
        this.containerClient = containerClient;
        this.options = options;
    }

    @Override
    public String upload(String payload) {
        String blobName = UUID.randomUUID().toString().replace("-", "");
        BlobClient blob = this.containerClient.getBlobClient(blobName);

        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);

        // Ensure container exists (idempotent)
        try {
            this.containerClient.createIfNotExists();
        } catch (BlobStorageException e) {
            // 409 Conflict means it already exists — safe to ignore
            if (e.getStatusCode() != 409) {
                throw new PayloadStorageException(
                    "Failed to create blob container '" + this.containerClient.getBlobContainerName() + "'.", e);
            }
        }

        try {
            if (this.options.isCompressionEnabled()) {
                ByteArrayOutputStream compressedBuffer = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(compressedBuffer)) {
                    gzip.write(payloadBytes);
                }
                byte[] compressedBytes = compressedBuffer.toByteArray();
                BlobHttpHeaders headers = new BlobHttpHeaders().setContentEncoding(CONTENT_ENCODING_GZIP);
                try (InputStream stream = new ByteArrayInputStream(compressedBytes)) {
                    blob.uploadWithResponse(
                        stream,
                        compressedBytes.length,
                        null, // parallelTransferOptions
                        headers,
                        null, // metadata
                        null, // tier
                        null, // requestConditions
                        null, // timeout
                        Context.NONE);
                }
            } else {
                try (InputStream stream = new ByteArrayInputStream(payloadBytes)) {
                    blob.upload(stream, payloadBytes.length, true);
                }
            }
        } catch (IOException e) {
            throw new PayloadStorageException("Failed to upload payload blob '" + blobName + "'.", e);
        }

        return encodeToken(this.containerClient.getBlobContainerName(), blobName);
    }

    @Override
    public String download(String token) {
        String[] decoded = decodeToken(token);
        String container = decoded[0];
        String name = decoded[1];

        if (!container.equals(this.containerClient.getBlobContainerName())) {
            throw new IllegalArgumentException("Token container does not match configured container.");
        }

        BlobClient blob = this.containerClient.getBlobClient(name);

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            // Use downloadStreamWithResponse to get content-encoding header in the same call,
            // avoiding a separate getProperties() round-trip.
            BlobDownloadResponse downloadResponse = blob.downloadStreamWithResponse(
                outputStream,
                null,  // range (full blob)
                null,  // options
                null,  // requestConditions
                false, // getMD5
                null,  // timeout
                Context.NONE);
            byte[] rawBytes = outputStream.toByteArray();

            // Check if the content is gzip-compressed via the response header
            String contentEncoding = downloadResponse.getDeserializedHeaders().getContentEncoding();
            boolean isGzip = CONTENT_ENCODING_GZIP.equalsIgnoreCase(contentEncoding);

            if (isGzip) {
                try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(rawBytes));
                     ByteArrayOutputStream decompressedBuffer = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[8192];
                    int len;
                    while ((len = gzip.read(buffer)) != -1) {
                        decompressedBuffer.write(buffer, 0, len);
                    }
                    return decompressedBuffer.toString(StandardCharsets.UTF_8.name());
                }
            }

            return new String(rawBytes, StandardCharsets.UTF_8);
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                throw new PayloadStorageException(
                    "The blob '" + name + "' was not found in container '" + container + "'. " +
                    "The payload may have been deleted or the container was never created.", e);
            }
            throw new PayloadStorageException("Failed to download payload blob '" + name + "'.", e);
        } catch (IOException e) {
            throw new PayloadStorageException("Failed to decompress payload blob '" + name + "'.", e);
        }
    }

    @Override
    public boolean isKnownPayloadToken(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        return value.startsWith(TOKEN_PREFIX);
    }

    static String encodeToken(String container, String name) {
        return TOKEN_PREFIX + container + ":" + name;
    }

    static String[] decodeToken(String token) {
        if (!token.startsWith(TOKEN_PREFIX)) {
            throw new IllegalArgumentException("Invalid external payload token.");
        }
        String rest = token.substring(TOKEN_PREFIX.length());
        int sep = rest.indexOf(':');
        if (sep <= 0 || sep >= rest.length() - 1) {
            throw new IllegalArgumentException("Invalid external payload token format.");
        }
        return new String[] { rest.substring(0, sep), rest.substring(sep + 1) };
    }
}
