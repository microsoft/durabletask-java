// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.credential.TokenCredential;

import javax.annotation.Nullable;
import java.net.URI;

/**
 * Configuration options for externalized payload storage using Azure Blob Storage.
 * <p>
 * Supports both connection string and identity-based (TokenCredential) authentication.
 * Either {@link #setConnectionString(String)} or both {@link #setAccountUri(URI)} and
 * {@link #setCredential(TokenCredential)} must be set before use.
 *
 * <p>Example (connection string):
 * <pre>{@code
 * LargePayloadStorageOptions options = new LargePayloadStorageOptions()
 *     .setConnectionString("UseDevelopmentStorage=true");
 * }</pre>
 *
 * <p>Example (identity-based):
 * <pre>{@code
 * LargePayloadStorageOptions options = new LargePayloadStorageOptions()
 *     .setAccountUri(new URI("https://mystorageaccount.blob.core.windows.net"))
 *     .setCredential(new DefaultAzureCredentialBuilder().build());
 * }</pre>
 */
public final class LargePayloadStorageOptions {

    private static final int ONE_MIB = 1024 * 1024;

    private int thresholdBytes = 900_000;
    private int maxPayloadBytes = 10 * ONE_MIB;
    private String connectionString;
    private URI accountUri;
    private TokenCredential credential;
    private String containerName = "durabletask-payloads";
    private boolean compressionEnabled = true;

    /**
     * Gets the threshold in bytes at which payloads are externalized to blob storage.
     * Payloads smaller than this threshold are sent inline. Default is 900,000 bytes.
     *
     * @return the threshold in bytes
     */
    public int getThresholdBytes() {
        return this.thresholdBytes;
    }

    /**
     * Sets the threshold in bytes at which payloads are externalized to blob storage.
     * Value must not exceed 1 MiB (1,048,576 bytes).
     *
     * @param thresholdBytes the threshold in bytes
     * @return this options object
     * @throws IllegalArgumentException if the value exceeds 1 MiB
     */
    public LargePayloadStorageOptions setThresholdBytes(int thresholdBytes) {
        if (thresholdBytes < 0) {
            throw new IllegalArgumentException(
                "Payload storage threshold cannot be negative.");
        }
        if (thresholdBytes > ONE_MIB) {
            throw new IllegalArgumentException(
                "Payload storage threshold cannot exceed 1 MiB (" + ONE_MIB + " bytes).");
        }
        this.thresholdBytes = thresholdBytes;
        return this;
    }

    /**
     * Gets the maximum allowed size in bytes for any single externalized payload.
     * Default is 10 MB. Requests exceeding this limit will fail fast with a clear error
     * to prevent unbounded payload growth and excessive storage/network usage.
     *
     * @return the maximum payload size in bytes
     */
    public int getMaxPayloadBytes() {
        return this.maxPayloadBytes;
    }

    /**
     * Sets the maximum allowed size in bytes for any single externalized payload.
     *
     * @param maxPayloadBytes the maximum payload size in bytes
     * @return this options object
     */
    public LargePayloadStorageOptions setMaxPayloadBytes(int maxPayloadBytes) {
        if (maxPayloadBytes <= 0) {
            throw new IllegalArgumentException(
                "Maximum payload size must be a positive number of bytes.");
        }
        this.maxPayloadBytes = maxPayloadBytes;
        return this;
    }

    /**
     * Gets the Azure Storage connection string.
     *
     * @return the connection string, or {@code null} if not set
     */
    @Nullable
    public String getConnectionString() {
        return this.connectionString;
    }

    /**
     * Sets the Azure Storage connection string.
     * Either this or {@link #setAccountUri(URI)} and {@link #setCredential(TokenCredential)} must be set.
     * Pass {@code null} to clear a previously-set value (for example, to switch to identity-based auth).
     *
     * @param connectionString the Azure Storage connection string, or {@code null} to clear
     * @return this options object
     */
    public LargePayloadStorageOptions setConnectionString(@Nullable String connectionString) {
        this.connectionString = connectionString;
        return this;
    }

    /**
     * Gets the Azure Storage account URI for identity-based authentication.
     *
     * @return the account URI, or {@code null} if not set
     */
    @Nullable
    public URI getAccountUri() {
        return this.accountUri;
    }

    /**
     * Sets the Azure Storage account URI for identity-based authentication.
     * Must be used together with {@link #setCredential(TokenCredential)}.
     * Pass {@code null} to clear a previously-set value.
     *
     * @param accountUri the Azure Storage account URI, or {@code null} to clear
     * @return this options object
     */
    public LargePayloadStorageOptions setAccountUri(@Nullable URI accountUri) {
        this.accountUri = accountUri;
        return this;
    }

    /**
     * Gets the credential for identity-based authentication.
     *
     * @return the credential, or {@code null} if not set
     */
    @Nullable
    public TokenCredential getCredential() {
        return this.credential;
    }

    /**
     * Sets the credential for identity-based authentication.
     * Must be used together with {@link #setAccountUri(URI)}.
     * Pass {@code null} to clear a previously-set value.
     *
     * @param credential the credential, or {@code null} to clear
     * @return this options object
     */
    public LargePayloadStorageOptions setCredential(@Nullable TokenCredential credential) {
        this.credential = credential;
        return this;
    }

    /**
     * Gets the blob container name used for externalized payloads.
     * Default is "durabletask-payloads".
     *
     * @return the container name
     */
    public String getContainerName() {
        return this.containerName;
    }

    /**
     * Sets the blob container name used for externalized payloads.
     *
     * @param containerName the container name
     * @return this options object
     */
    public LargePayloadStorageOptions setContainerName(String containerName) {
        this.containerName = containerName;
        return this;
    }

    /**
     * Gets whether payloads should be gzip-compressed when stored.
     * Default is {@code true} for reduced storage and bandwidth.
     *
     * @return {@code true} if compression is enabled
     */
    public boolean isCompressionEnabled() {
        return this.compressionEnabled;
    }

    /**
     * Sets whether payloads should be gzip-compressed when stored.
     *
     * @param compressionEnabled {@code true} to enable gzip compression
     * @return this options object
     */
    public LargePayloadStorageOptions setCompressionEnabled(boolean compressionEnabled) {
        this.compressionEnabled = compressionEnabled;
        return this;
    }
}
