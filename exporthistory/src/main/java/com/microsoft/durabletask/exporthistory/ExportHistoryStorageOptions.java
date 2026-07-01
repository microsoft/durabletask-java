// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.azure.core.credential.TokenCredential;

import javax.annotation.Nullable;
import java.net.URI;

/**
 * Configuration for the Azure Blob Storage destination of an export history job.
 * <p>
 * Supports both connection-string and identity-based ({@link TokenCredential}) authentication. Either
 * {@link #setConnectionString(String)} or both {@link #setAccountUri(URI)} and
 * {@link #setCredential(TokenCredential)} must be set before use.
 *
 * <p>Example (connection string):
 * <pre>{@code
 * ExportHistoryStorageOptions storage = new ExportHistoryStorageOptions()
 *     .setConnectionString(System.getenv("EXPORT_HISTORY_STORAGE_CONNECTION_STRING"))
 *     .setContainerName("orchestration-history")
 *     .setPrefix("exports/");
 * }</pre>
 *
 * <p>Example (identity-based):
 * <pre>{@code
 * ExportHistoryStorageOptions storage = new ExportHistoryStorageOptions()
 *     .setAccountUri(new URI("https://mystorageaccount.blob.core.windows.net"))
 *     .setCredential(new DefaultAzureCredentialBuilder().build())
 *     .setContainerName("orchestration-history");
 * }</pre>
 */
public final class ExportHistoryStorageOptions {

    private String connectionString;
    private URI accountUri;
    private TokenCredential credential;
    private String containerName = "";
    private String prefix;
    private ExportFormat format = ExportFormat.getDefault();

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
     * Sets the Azure Storage connection string. Either this or {@link #setAccountUri(URI)} +
     * {@link #setCredential(TokenCredential)} must be set.
     *
     * @param connectionString the connection string, or {@code null} to clear
     * @return this options object
     */
    public ExportHistoryStorageOptions setConnectionString(@Nullable String connectionString) {
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
     * Sets the Azure Storage account URI for identity-based authentication. Must be used together with
     * {@link #setCredential(TokenCredential)}.
     *
     * @param accountUri the account URI, or {@code null} to clear
     * @return this options object
     */
    public ExportHistoryStorageOptions setAccountUri(@Nullable URI accountUri) {
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
     * Sets the credential for identity-based authentication. Must be used together with
     * {@link #setAccountUri(URI)}.
     *
     * @param credential the credential, or {@code null} to clear
     * @return this options object
     */
    public ExportHistoryStorageOptions setCredential(@Nullable TokenCredential credential) {
        this.credential = credential;
        return this;
    }

    /**
     * Gets the blob container name where exported history is stored.
     *
     * @return the container name
     */
    public String getContainerName() {
        return this.containerName;
    }

    /**
     * Sets the blob container name where exported history is stored.
     *
     * @param containerName the container name
     * @return this options object
     */
    public ExportHistoryStorageOptions setContainerName(String containerName) {
        this.containerName = containerName;
        return this;
    }

    /**
     * Gets the optional blob path prefix.
     *
     * @return the prefix, or {@code null} if not set
     */
    @Nullable
    public String getPrefix() {
        return this.prefix;
    }

    /**
     * Sets an optional prefix for exported blob paths.
     *
     * @param prefix the prefix, or {@code null} to clear
     * @return this options object
     */
    public ExportHistoryStorageOptions setPrefix(@Nullable String prefix) {
        this.prefix = prefix;
        return this;
    }

    /**
     * Gets the export format. Defaults to {@link ExportFormat#getDefault()} (JSONL + gzip).
     *
     * @return the export format
     */
    public ExportFormat getFormat() {
        return this.format;
    }

    /**
     * Sets the export format.
     *
     * @param format the export format
     * @return this options object
     */
    public ExportHistoryStorageOptions setFormat(ExportFormat format) {
        this.format = format;
        return this;
    }
}
