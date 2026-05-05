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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
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

    // Blob name is UUID.randomUUID().toString().replace("-", ""): exactly 32 lowercase hex chars.
    // Container name follows Azure rules: 3-63 chars, lowercase alphanumerics and single hyphens,
    // must start and end with alphanumeric (see isValidContainerName).
    // Full token grammar: blob:v1:<container>:<32-lowercase-hex>
    private static final Pattern TOKEN_PATTERN = Pattern.compile(
        "^blob:v1:[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){1,61}[a-z0-9]:[0-9a-f]{32}$");

    private final BlobContainerClient containerClient;
    private final LargePayloadStorageOptions options;

    // Container-creation guard. The first thread to call ensureContainerExists() creates the
    // latch and performs the RPC. Concurrent callers await the latch so they don't race ahead
    // and upload to a container that hasn't been created yet. On failure the reference is
    // reset to null so a subsequent call can retry.
    private final AtomicReference<CountDownLatch> containerLatch = new AtomicReference<>();
    private volatile boolean containerVerified;

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

        // Ensure container exists before uploading. Thread-safe: the first caller creates
        // the container while concurrent callers wait for it to complete.
        ensureContainerExists();

        try {
            // Defense-in-depth: require the blob to not already exist (If-None-Match: *).
            // Blob names are random UUIDs so collisions are astronomically unlikely, but this
            // guards against future regressions (e.g. a caller-supplied PayloadStore that
            // generates deterministic names or a refactor that reuses names) by failing loudly
            // instead of silently overwriting someone else's payload.
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
                        stream,
                        compressedBytes.length,
                        null,       // parallelTransferOptions
                        headers,
                        null,       // metadata
                        null,       // tier
                        conditions, // requestConditions
                        null,       // timeout
                        Context.NONE);
                }
            } else {
                try (InputStream stream = new ByteArrayInputStream(payloadBytes)) {
                    blob.uploadWithResponse(
                        stream,
                        payloadBytes.length,
                        null,       // parallelTransferOptions
                        null,       // headers
                        null,       // metadata
                        null,       // tier
                        conditions, // requestConditions
                        null,       // timeout
                        Context.NONE);
                }
            }
        } catch (BlobStorageException e) {
            // 409 BlobAlreadyExists and 412 ConditionNotMet (from If-None-Match: *) both indicate
            // a name collision on upload — treat as a hard failure rather than silently overwriting.
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

        return encodeToken(this.containerClient.getBlobContainerName(), blobName);
    }

    /**
     * Ensures the blob container exists, creating it if necessary. Thread-safe: the first
     * caller performs the RPC while concurrent callers wait for it to complete. On success
     * the check is skipped on all future calls. On failure the guard is reset so a later
     * call can retry.
     */
    private void ensureContainerExists() {
        if (this.containerVerified) {
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        if (!this.containerLatch.compareAndSet(null, latch)) {
            CountDownLatch existing = this.containerLatch.get();
            if (existing == null) {
                // Rare race: the creating thread already reset the latch (failure path).
                // Retry on the next upload call rather than proceeding without a container.
                return;
            }
            // Another thread is already creating the container — wait for it.
            try {
                existing.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PayloadStorageException("Interrupted while waiting for container creation.", e);
            }
            // If the creating thread failed, containerVerified is still false; the next
            // upload attempt will retry. For now, return and let the upload proceed
            // (it will fail fast with a clear error if the container doesn't exist).
            return;
        }

        // This thread is responsible for creating the container.
        try {
            this.containerClient.createIfNotExists();
            this.containerVerified = true;
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 409) {
                // 409 Conflict means it already exists — safe to ignore.
                this.containerVerified = true;
            } else {
                this.containerLatch.set(null); // allow a future call to retry
                throw new PayloadStorageException(
                    "Failed to create blob container '" + this.containerClient.getBlobContainerName() + "'.", e);
            }
        } catch (RuntimeException e) {
            this.containerLatch.set(null); // allow a future call to retry
            throw e;
        } finally {
            latch.countDown(); // unblock waiting threads
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
