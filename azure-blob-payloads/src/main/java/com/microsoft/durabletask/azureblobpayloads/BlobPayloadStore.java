// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.microsoft.durabletask.PayloadStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Azure Blob Storage implementation of {@link PayloadStore}.
 * <p>
 * This class uploads large payloads to Azure Blob Storage and returns tokens
 * in the format {@code blob:v1:<container>:<blobName>} that can be recognized
 * and resolved by this store.
 * <p>
 * The store automatically creates the container if it does not exist.
 * Optionally compresses payloads with gzip (enabled by default).
 *
 * @see BlobPayloadStoreOptions
 * @see PayloadStore
 */
public final class BlobPayloadStore implements PayloadStore {

    private static final Logger logger = Logger.getLogger(BlobPayloadStore.class.getName());
    private static final String BLOB_EXTENSION = ".json";
    private static final String TOKEN_PREFIX = "blob:v1:";
    private static final String GZIP_CONTENT_ENCODING = "gzip";

    private final BlobContainerClient containerClient;
    private final String blobPrefix;
    private final String containerName;
    private final boolean compressPayloads;
    private volatile boolean containerEnsured;

    /**
     * Creates a new BlobPayloadStore with the given options.
     *
     * @param options the blob payload store configuration
     */
    public BlobPayloadStore(BlobPayloadStoreOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null");
        }

        BlobServiceClient serviceClient;
        if (options.getBlobServiceClient() != null) {
            serviceClient = options.getBlobServiceClient();
        } else if (options.getConnectionString() != null) {
            serviceClient = new BlobServiceClientBuilder()
                .connectionString(options.getConnectionString())
                .buildClient();
        } else {
            serviceClient = new BlobServiceClientBuilder()
                .endpoint(options.getBlobServiceEndpoint())
                .credential(options.getCredential())
                .buildClient();
        }

        this.containerClient = serviceClient.getBlobContainerClient(options.getContainerName());
        this.blobPrefix = options.getBlobPrefix();
        this.containerName = options.getContainerName();
        this.compressPayloads = options.isCompressPayloads();
    }

    @Override
    public String upload(String payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }

        ensureContainerExists();

        String blobName = this.blobPrefix + UUID.randomUUID().toString().replace("-", "") + BLOB_EXTENSION;
        BlobClient blobClient = this.containerClient.getBlobClient(blobName);

        byte[] rawData = payload.getBytes(StandardCharsets.UTF_8);
        byte[] data;

        if (this.compressPayloads) {
            data = gzipCompress(rawData);
            BlobHttpHeaders headers = new BlobHttpHeaders().setContentEncoding(GZIP_CONTENT_ENCODING);
            BlobParallelUploadOptions uploadOptions = new BlobParallelUploadOptions(
                new ByteArrayInputStream(data), data.length)
                .setHeaders(headers);
            blobClient.uploadWithResponse(uploadOptions, null, null);
        } else {
            data = rawData;
            blobClient.upload(new ByteArrayInputStream(data), data.length, true);
        }

        String token = TOKEN_PREFIX + this.containerName + ":" + blobName;
        logger.fine(() -> String.format("Uploaded payload (%d bytes, compressed=%s) to %s",
            rawData.length, this.compressPayloads, token));
        return token;
    }

    @Override
    public String download(String token) {
        if (token == null || token.isEmpty()) {
            throw new IllegalArgumentException("token must not be null or empty");
        }

        String blobName = extractBlobName(token);
        BlobClient blobClient = this.containerClient.getBlobClient(blobName);

        BlobDownloadContentResponse response = blobClient.downloadContentWithResponse(null, null, null, null);
        byte[] rawBytes = response.getValue().toBytes();
        String contentEncoding = response.getDeserializedHeaders().getContentEncoding();
        if (GZIP_CONTENT_ENCODING.equalsIgnoreCase(contentEncoding)) {
            rawBytes = gzipDecompress(rawBytes);
        }

        String payload = new String(rawBytes, StandardCharsets.UTF_8);
        logger.fine(() -> String.format("Downloaded payload (%d bytes) from %s", payload.length(), token));
        return payload;
    }

    @Override
    public boolean isKnownPayloadToken(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        return value.startsWith(TOKEN_PREFIX);
    }

    /**
     * Ensures the blob container exists, creating it if necessary.
     * The check-then-act on {@code containerEnsured} is intentionally non-atomic:
     * concurrent callers may race through to {@code create()}, but the 409 Conflict
     * handler makes this benign.
     */
    private void ensureContainerExists() {
        if (this.containerEnsured) {
            return;
        }
        try {
            if (!this.containerClient.exists()) {
                this.containerClient.create();
                logger.info(() -> String.format("Created blob container: %s", this.containerClient.getBlobContainerName()));
            }
            this.containerEnsured = true;
        } catch (BlobStorageException e) {
            // Container might have been created concurrently (409 Conflict)
            if (e.getStatusCode() != 409) {
                throw e;
            }
            this.containerEnsured = true;
        }
    }

    /**
     * Extracts the blob name from a {@code blob:v1:<container>:<blobName>} token
     * and validates that the container matches the configured container.
     */
    private String extractBlobName(String token) {
        if (!token.startsWith(TOKEN_PREFIX)) {
            throw new IllegalArgumentException(
                "Token does not have the expected format (blob:v1:...): " + token);
        }
        // Format: blob:v1:<container>:<blobName>
        String remainder = token.substring(TOKEN_PREFIX.length());
        int colonIndex = remainder.indexOf(':');
        if (colonIndex < 0) {
            throw new IllegalArgumentException(
                "Token does not have the expected format (blob:v1:<container>:<blobName>): " + token);
        }
        String tokenContainer = remainder.substring(0, colonIndex);
        if (!this.containerName.equals(tokenContainer)) {
            throw new IllegalArgumentException(String.format(
                "Token container '%s' does not match configured container '%s'",
                tokenContainer, this.containerName));
        }
        return remainder.substring(colonIndex + 1);
    }

    private static byte[] gzipCompress(byte[] data) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
                gzipOut.write(data);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to gzip compress payload", e);
        }
    }

    /**
     * Maximum decompressed size (20 MiB) to guard against decompression bombs.
     * This is 2x the default max externalized payload size of 10 MiB.
     */
    private static final int MAX_DECOMPRESSED_BYTES = 20 * 1024 * 1024;

    private static byte[] gzipDecompress(byte[] compressed) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
                byte[] buffer = new byte[8192];
                int len;
                int totalRead = 0;
                while ((len = gzipIn.read(buffer)) != -1) {
                    totalRead += len;
                    if (totalRead > MAX_DECOMPRESSED_BYTES) {
                        throw new IOException(
                            "Decompressed payload exceeds safety limit of " + MAX_DECOMPRESSED_BYTES + " bytes");
                    }
                    baos.write(buffer, 0, len);
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to gzip decompress payload", e);
        }
    }
}
