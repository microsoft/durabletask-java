// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.zip.GZIPOutputStream;

/**
 * Writes serialized orchestration history to Azure Blob Storage.
 * <p>
 * Built once from {@link ExportHistoryStorageOptions} (connection-string or identity auth) and reused across export
 * activities. The target container is taken from each {@link ExportDestination}; the container is created on first
 * use.
 */
final class BlobExportWriter {

    private final BlobServiceClient serviceClient;

    /**
     * Creates a {@code BlobExportWriter} from storage options.
     *
     * @param options the storage options (connection string, or account URI + credential)
     * @throws IllegalArgumentException if neither connection string nor account URI/credential are provided
     */
    BlobExportWriter(ExportHistoryStorageOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null.");
        }

        boolean hasConnectionString = options.getConnectionString() != null
                && !options.getConnectionString().isEmpty();
        boolean hasIdentityAuth = options.getAccountUri() != null && options.getCredential() != null;

        if (!hasConnectionString && !hasIdentityAuth) {
            throw new IllegalArgumentException(
                    "Either ConnectionString or AccountUri and Credential must be provided.");
        }

        // Exponential retry, matching the azure-blob-payloads BlobPayloadStore configuration.
        RequestRetryOptions retryOptions = new RequestRetryOptions(
                RetryPolicyType.EXPONENTIAL,
                8,
                120,
                250L,
                10_000L,
                null);

        if (hasIdentityAuth) {
            this.serviceClient = new BlobServiceClientBuilder()
                    .endpoint(options.getAccountUri().toString())
                    .credential(options.getCredential())
                    .retryOptions(retryOptions)
                    .buildClient();
        } else {
            this.serviceClient = new BlobServiceClientBuilder()
                    .connectionString(options.getConnectionString())
                    .retryOptions(retryOptions)
                    .buildClient();
        }
    }

    /** Package-private constructor for testing with an injected service client. */
    BlobExportWriter(BlobServiceClient serviceClient) {
        this.serviceClient = serviceClient;
    }

    /**
     * Uploads serialized history content to a blob, creating the container if needed and overwriting any existing
     * blob with the same name.
     *
     * @param containerName the target container
     * @param blobPath      the blob path (including any prefix)
     * @param content       the serialized content
     * @param format        the export format (determines gzip + content type)
     * @param instanceId    the instance ID, recorded as blob metadata
     */
    void upload(String containerName, String blobPath, String content, ExportFormat format, String instanceId) {
        if (containerName == null || containerName.isEmpty()) {
            throw new IllegalArgumentException("Blob container name must not be null or empty.");
        }
        if (blobPath == null || blobPath.isEmpty()) {
            throw new IllegalArgumentException("Blob path must not be null or empty.");
        }
        BlobContainerClient containerClient = this.serviceClient.getBlobContainerClient(containerName);
        containerClient.createIfNotExists();

        BlobClient blobClient = containerClient.getBlobClient(blobPath);

        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        boolean gzip = HistoryEventSerializer.isCompressed(format);
        byte[] payload = gzip ? gzip(contentBytes) : contentBytes;

        blobClient.upload(BinaryData.fromBytes(payload), true);
        blobClient.setHttpHeaders(new BlobHttpHeaders()
                .setContentType(HistoryEventSerializer.contentType(format))
                .setContentEncoding(gzip ? "gzip" : null));
        blobClient.setMetadata(Collections.singletonMap("instanceId", instanceId));
    }

    private static byte[] gzip(byte[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipStream = new GZIPOutputStream(out)) {
            gzipStream.write(data);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to gzip export content.", e);
        }
        return out.toByteArray();
    }
}
