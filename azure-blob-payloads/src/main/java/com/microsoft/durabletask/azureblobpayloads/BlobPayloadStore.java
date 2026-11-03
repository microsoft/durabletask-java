// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobDownloadHeaders;
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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Azure Blob Storage implementation of {@link PayloadStore}.
 * <p>
 * Stores payloads as blobs and returns self-describing tokens in the form {@code blob:v2:<absolute-blob-URL>}.
 * Reads both v2 and legacy {@code blob:v1:<container>:<blobName>} tokens from the configured storage location.
 * Supports optional gzip compression. The blob container is created automatically on first upload.
 * If the container is subsequently deleted, a new upload recreates it and retries once.
 */
public final class BlobPayloadStore extends PayloadStore {

    static final String TOKEN_PREFIX = "blob:v1:";
    static final String TOKEN_PREFIX_V2 = "blob:v2:";
    private static final String CONTENT_ENCODING_GZIP = "gzip";

    // Blob name is UUID.randomUUID().toString().replace("-", ""): exactly 32 lowercase hex chars.
    // Container name follows Azure rules: 3-63 chars, lowercase alphanumerics and single hyphens,
    // must start and end with alphanumeric, or the special $root container.
    // Full token grammar: blob:v1:<container>:<32-lowercase-hex>
    private static final Pattern TOKEN_PATTERN = Pattern.compile(
        "^blob:v1:(?:\\$root|[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){1,61}[a-z0-9]):[0-9a-f]{32}$");

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
    public int getEstimatedTokenSizeBytes() {
        String url = this.containerClient.getBlobClient("00000000000000000000000000000000").getBlobUrl();
        return encodeToken(url).getBytes(StandardCharsets.UTF_8).length + 16;
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
                return encodeToken(blob.getBlobUrl());
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
                try {
                    blob.uploadWithResponse(
                        stream, compressedBytes.length, null, headers, null, null, conditions, null, Context.NONE);
                } catch (BlobStorageException e) {
                    if (isPossibleCommittedUpload(e) && blobContentMatches(blob, compressedBytes, CONTENT_ENCODING_GZIP)) {
                        return;
                    }
                    throw e;
                }
            }
        } else {
            try (InputStream stream = new ByteArrayInputStream(payloadBytes)) {
                try {
                    blob.uploadWithResponse(
                        stream, payloadBytes.length, null, null, null, null, conditions, null, Context.NONE);
                } catch (BlobStorageException e) {
                    if (isPossibleCommittedUpload(e) && blobContentMatches(blob, payloadBytes, null)) {
                        return;
                    }
                    throw e;
                }
            }
        }
    }

    private static boolean isPossibleCommittedUpload(BlobStorageException e) {
        return e.getStatusCode() == 412
            || (e.getStatusCode() == 409 && BlobErrorCode.BLOB_ALREADY_EXISTS.equals(e.getErrorCode()));
    }

    private static boolean blobContentMatches(BlobClient blob, byte[] expectedBytes, String expectedContentEncoding) {
        // Verification failures must propagate with their own status/cause. In particular,
        // a transient GET failure must not turn the ambiguous PUT into a permanent conflict.
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BlobDownloadResponse response = blob.downloadStreamWithResponse(
            output,
            null,
            null,
            null,
            false,
            null,
            Context.NONE);
        BlobDownloadHeaders headers = response.getDeserializedHeaders();
        String actualEncoding = headers == null ? null : headers.getContentEncoding();
        if (expectedContentEncoding == null) {
            if (actualEncoding != null && !actualEncoding.isEmpty()) {
                return false;
            }
        } else if (!expectedContentEncoding.equalsIgnoreCase(actualEncoding)) {
            return false;
        }
        return Arrays.equals(expectedBytes, output.toByteArray());
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
        if (!isKnownPayloadToken(token)) {
            throw new IllegalArgumentException("Invalid external payload token.");
        }
        String[] decoded = decodeToken(token);
        String container = decoded[0];
        String name = decoded[1];

        if (!container.equals(this.containerClient.getBlobContainerName())) {
            throw new IllegalArgumentException("Token container does not match configured container.");
        }

        BlobClient blob = this.containerClient.getBlobClient(name);
        if (token.startsWith(TOKEN_PREFIX_V2)
                && !sameDestination(decodeV2Token(token), URI.create(blob.getBlobUrl()))) {
            throw new IllegalArgumentException("Token endpoint or container does not match configured storage location.");
        }

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
        if (value.startsWith(TOKEN_PREFIX_V2)) {
            try {
                decodeV2Token(value);
                return true;
            } catch (IllegalArgumentException ex) {
                return false;
            }
        }
        return TOKEN_PATTERN.matcher(value).matches();
    }

    static String encodeToken(String blobUrl) {
        // Authentication stays in the configured pipeline, never in persisted tokens.
        String token = TOKEN_PREFIX_V2 + blobUrl.split("[?#]", 2)[0];
        decodeV2Token(token);
        return token;
    }

    static String encodeToken(String container, String name) {
        return TOKEN_PREFIX + container + ":" + name;
    }

    static String[] decodeToken(String token) {
        if (token.startsWith(TOKEN_PREFIX_V2)) {
            String path = decodeV2Token(token).getRawPath();
            int nameSeparator = path.lastIndexOf('/');
            int containerSeparator = path.lastIndexOf('/', nameSeparator - 1);
            return new String[] {
                path.substring(containerSeparator + 1, nameSeparator), path.substring(nameSeparator + 1)
            };
        }
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

    private static URI decodeV2Token(String token) {
        URI uri = URI.create(token.substring(TOKEN_PREFIX_V2.length()));
        String path = uri.getRawPath();
        if (!uri.isAbsolute() || uri.getHost() == null
                || !("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
                || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null
                || uri.getPort() > 65535 || path == null || !path.equals(uri.normalize().getRawPath())) {
            throw new IllegalArgumentException("Invalid external payload URL.");
        }
        int separator = path.lastIndexOf('/');
        int containerSeparator = path.lastIndexOf('/', separator - 1);
        if (containerSeparator < 0
                || !TOKEN_PATTERN.matcher(TOKEN_PREFIX + path.substring(containerSeparator + 1, separator)
                    + ":" + path.substring(separator + 1)).matches()) {
            throw new IllegalArgumentException("Invalid external payload URL path.");
        }
        return uri;
    }

    private static boolean sameDestination(URI token, URI configured) {
        return token.getScheme().equalsIgnoreCase(configured.getScheme())
            && token.getHost().equalsIgnoreCase(configured.getHost())
            && effectivePort(token) == effectivePort(configured)
            && token.normalize().getRawPath().equals(configured.normalize().getRawPath());
    }

    private static int effectivePort(URI uri) {
        return uri.getPort() >= 0 ? uri.getPort() : ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80);
    }
}
