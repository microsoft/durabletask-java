// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.credential.TokenCredential;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    private int thresholdBytes = 256 * 1024;
    private int maxPayloadBytes = 10 * ONE_MIB;
    private String connectionString;
    private URI accountUri;
    private TokenCredential credential;
    private String containerName = "durabletask-payloads";
    private boolean compressionEnabled = true;
    private boolean allowAllContainersInConfiguredAccount;
    private List<URI> additionalAccountUris = Collections.emptyList();

    /**
     * Gets the threshold in bytes at which payloads are externalized to blob storage.
     * Payloads smaller than this threshold are sent inline. Default is 256 KiB (262,144 bytes).
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
     * Gets whether reads may access all containers in the configured account.
     * The default is {@code false}, restricting reads to the configured container.
     *
     * @return whether primary-account-wide access is enabled
     */
    public boolean isAllowAllContainersInConfiguredAccount() {
        return this.allowAllContainersInConfiguredAccount;
    }

    /**
     * Allows reads from all containers in the configured account when enabled.
     * This does not allow other account endpoints or change Storage permissions.
     * Connection-string and identity authentication are both supported.
     *
     * @param enabled whether to allow primary-account-wide reads
     * @return this options object
     */
    public LargePayloadStorageOptions setAllowAllContainersInConfiguredAccount(boolean enabled) {
        this.allowAllContainersInConfiguredAccount = enabled;
        return this;
    }

    /**
     * Gets the additional account endpoints whose containers may be read.
     * Returns a fresh immutable snapshot, empty by default.
     *
     * @return the additional account URIs
     */
    public List<URI> getAdditionalAccountUris() {
        return Collections.unmodifiableList(new ArrayList<>(this.additionalAccountUris));
    }

    /**
     * Allows reads from all containers at the specified additional account endpoints.
     * Each URI identifies an SDK-recognized Blob account service endpoint, including its scheme, host,
     * effective port, and any account path, not a hostname or container URL.
     * Endpoints must use HTTPS. Requires {@link #setAccountUri(URI)} and {@link #setCredential(TokenCredential)};
     * connection-string credentials cannot be reused for additional accounts.
     * Primary-account container access remains controlled by
     * {@link #setAllowAllContainersInConfiguredAccount(boolean)}. Uploads stay in the configured container.
     * Invalid service endpoints and entries equivalent to the primary endpoint are rejected when the
     * store is constructed; equivalent additional entries are deduplicated.
     * Read scopes are copied when a {@link BlobPayloadStore} is constructed.
     *
     * @param accountUris additional endpoints, or an empty list to clear them
     * @return this options object
     * @throws IllegalArgumentException if the list or any element is null
     */
    public LargePayloadStorageOptions setAdditionalAccountUris(List<URI> accountUris) {
        if (accountUris == null) {
            throw new IllegalArgumentException("Additional account URIs must not be null.");
        }
        for (URI accountUri : accountUris) {
            if (accountUri == null) {
                throw new IllegalArgumentException("Additional account URIs must not contain null.");
            }
        }
        this.additionalAccountUris = Collections.unmodifiableList(new ArrayList<>(accountUris));
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
