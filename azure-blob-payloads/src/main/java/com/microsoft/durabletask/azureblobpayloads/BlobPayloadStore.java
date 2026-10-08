// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Azure Blob Storage implementation of {@link PayloadStore}.
 * <p>
 * Stores payloads as blobs and returns opaque tokens in the form {@code blob:v1:<container>:<blobName>}.
 * Supports optional gzip compression. The blob container is created automatically on first upload.
 * If the container is subsequently deleted, a new upload recreates it and retries once.
 */
public final class BlobPayloadStore extends PayloadStore {

    static final String TOKEN_PREFIX = "blob:v1:";
    private static final String CONTENT_ENCODING_GZIP = "gzip";

    // Blob name is UUID.randomUUID().toString().replace("-", ""): exactly 32 lowercase hex chars.
    // Container name follows Azure rules: 3-63 chars, lowercase alphanumerics and single hyphens,
    // must start and end with alphanumeric (see isValidContainerName).
    // Full token grammar: blob:v1:<container>:<32-lowercase-hex>
    private static final Pattern TOKEN_PATTERN = Pattern.compile(
        "^blob:v1:[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){1,61}[a-z0-9]:[0-9a-f]{32}$");

    private final BlobContainerClient containerClient;
    private final LargePayloadStorageOptions options;

    private final ReentrantLock containerInitializationLock = new ReentrantLock();
    // A stale upload failure must not invalidate a container another upload has already recreated.
    private final AtomicReference<Object> containerGeneration = new AtomicReference<>();

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

        boolean retryAfterContainerNotFound = true;
        while (true) {
            Object generation = ensureContainerExists();
            try {
                uploadBlob(blob, payloadBytes);
                return encodeToken(this.containerClient.getBlobContainerName(), blobName);
            } catch (BlobStorageException e) {
                if (retryAfterContainerNotFound
                        && e.getStatusCode() == 404
                        && BlobErrorCode.CONTAINER_NOT_FOUND.equals(e.getErrorCode())) {
                    this.containerGeneration.compareAndSet(generation, null);
                    retryAfterContainerNotFound = false;
                    continue;
                }

                if (e.getStatusCode() == 409 || e.getStatusCode() == 412) {
                    throw new PayloadStorageException(
                        "Payload blob '" + blobName + "' already exists in container '"
                            + this.containerClient.getBlobContainerName()
                            + "'. Refusing to overwrite. This should not happen with random UUID blob names "
                            + "and likely indicates a bug in a custom PayloadStore implementation.", e);
                }
                throw new PayloadStorageException("Failed to upload payload blob '" + blobName + "'.", e);
            } catch (IOException e) {
                throw new PayloadStorageException("Failed to upload payload blob '" + blobName + "'.", e);
            }
        }
    }

    private void uploadBlob(BlobClient blob, byte[] payloadBytes) throws IOException {
        BlobRequestConditions conditions = new BlobRequestConditions().setIfNoneMatch("*");
        if (this.options.isCompressionEnabled()) {
            ByteArrayOutputStream compressedBuffer = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(compressedBuffer)) {
                gzip.write(payloadBytes);
            }
            byte[] compressedBytes = compressedBuffer.toByteArray();
            BlobHttpHeaders headers = new BlobHttpHeaders().setContentEncoding(CONTENT_ENCODING_GZIP);
            try (InputStream stream = new ByteArrayInputStream(compressedBytes)) {
                blob.uploadWithResponse(
                    stream, compressedBytes.length, null, headers, null, null, conditions, null, Context.NONE);
            }
        } else {
            try (InputStream stream = new ByteArrayInputStream(payloadBytes)) {
                blob.uploadWithResponse(
                    stream, payloadBytes.length, null, null, null, null, conditions, null, Context.NONE);
            }
        }
    }

    private Object ensureContainerExists() {
        Object generation = this.containerGeneration.get();
        if (generation != null) {
            return generation;
        }

        try {
            this.containerInitializationLock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PayloadStorageException("Interrupted while waiting for container creation.", e);
        }

        try {
            generation = this.containerGeneration.get();
            if (generation == null) {
                try {
                    this.containerClient.createIfNotExists();
                } catch (BlobStorageException e) {
                    if (e.getStatusCode() != 409
                            || !BlobErrorCode.CONTAINER_ALREADY_EXISTS.equals(e.getErrorCode())) {
                        throw new PayloadStorageException(
                            "Failed to create blob container '" + this.containerClient.getBlobContainerName() + "'.", e);
                    }
                }
                generation = new Object();
                this.containerGeneration.set(generation);
            }
            return generation;
        } finally {
            this.containerInitializationLock.unlock();
        }
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
        // Validate the full token grammar (prefix + container + blob name), not just the
        // prefix, so arbitrary user strings that happen to start with "blob:v1:" are not
        // treated as tokens. This avoids spurious blob GETs (DoS surface) and spurious
        // "container mismatch" failures on the response path.
        if (value.length() < TOKEN_PREFIX.length() || !value.startsWith(TOKEN_PREFIX)) {
            return false;
        }
        return TOKEN_PATTERN.matcher(value).matches();
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
