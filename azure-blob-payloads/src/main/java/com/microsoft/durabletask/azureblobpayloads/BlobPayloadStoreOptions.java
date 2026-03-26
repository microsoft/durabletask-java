// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.BlobServiceClient;

/**
 * Configuration options for {@link BlobPayloadStore}.
 * <p>
 * Use the {@link Builder} to construct instances. Either a connection string or a
 * blob service endpoint with a credential must be provided.
 * <p>
 * Example using connection string:
 * <pre>{@code
 * BlobPayloadStoreOptions options = new BlobPayloadStoreOptions.Builder()
 *     .setConnectionString("DefaultEndpointsProtocol=https;...")
 *     .setContainerName("large-payloads")
 *     .build();
 * }</pre>
 * <p>
 * Example using endpoint with managed identity:
 * <pre>{@code
 * BlobPayloadStoreOptions options = new BlobPayloadStoreOptions.Builder()
 *     .setBlobServiceEndpoint("https://myaccount.blob.core.windows.net")
 *     .setCredential(new DefaultAzureCredentialBuilder().build())
 *     .setContainerName("large-payloads")
 *     .build();
 * }</pre>
 *
 * @see BlobPayloadStore
 */
public final class BlobPayloadStoreOptions {

    /**
     * Default container name for storing externalized payloads.
     */
    static final String DEFAULT_CONTAINER_NAME = "durabletask-payloads";

    /**
     * Default prefix for blob names.
     */
    static final String DEFAULT_BLOB_PREFIX = "payloads/";

    private final String connectionString;
    private final String blobServiceEndpoint;
    private final TokenCredential credential;
    private final BlobServiceClient blobServiceClient;
    private final String containerName;
    private final String blobPrefix;
    private final boolean compressPayloads;

    private BlobPayloadStoreOptions(Builder builder) {
        this.connectionString = builder.connectionString;
        this.blobServiceEndpoint = builder.blobServiceEndpoint;
        this.credential = builder.credential;
        this.blobServiceClient = builder.blobServiceClient;
        this.containerName = builder.containerName;
        this.blobPrefix = builder.blobPrefix;
        this.compressPayloads = builder.compressPayloads;
    }

    /**
     * Gets the Azure Storage connection string, if configured.
     *
     * @return the connection string, or null if endpoint-based auth is used
     */
    public String getConnectionString() {
        return this.connectionString;
    }

    /**
     * Gets the Azure Blob Storage service endpoint, if configured.
     *
     * @return the blob service endpoint, or null if connection string auth is used
     */
    public String getBlobServiceEndpoint() {
        return this.blobServiceEndpoint;
    }

    /**
     * Gets the token credential for authenticating to Azure Blob Storage, if configured.
     *
     * @return the token credential, or null
     */
    public TokenCredential getCredential() {
        return this.credential;
    }

    /**
     * Gets a pre-configured BlobServiceClient, if provided.
     *
     * @return the blob service client, or null
     */
    public BlobServiceClient getBlobServiceClient() {
        return this.blobServiceClient;
    }

    /**
     * Gets the container name for storing externalized payloads.
     *
     * @return the container name
     */
    public String getContainerName() {
        return this.containerName;
    }

    /**
     * Gets the blob name prefix for externalized payloads.
     *
     * @return the blob prefix
     */
    public String getBlobPrefix() {
        return this.blobPrefix;
    }

    /**
     * Gets whether payloads should be compressed with gzip before uploading.
     *
     * @return true if compression is enabled, false otherwise
     */
    public boolean isCompressPayloads() {
        return this.compressPayloads;
    }

    /**
     * Builder for constructing {@link BlobPayloadStoreOptions} instances.
     */
    public static final class Builder {
        private String connectionString;
        private String blobServiceEndpoint;
        private TokenCredential credential;
        private BlobServiceClient blobServiceClient;
        private String containerName = DEFAULT_CONTAINER_NAME;
        private String blobPrefix = DEFAULT_BLOB_PREFIX;
        private boolean compressPayloads = true;

        /**
         * Sets the Azure Storage connection string. Mutually exclusive with
         * {@link #setBlobServiceEndpoint(String)} and {@link #setBlobServiceClient(BlobServiceClient)}.
         * Setting this clears any previously set endpoint or pre-configured client.
         *
         * @param connectionString the connection string
         * @return this builder
         */
        public Builder setConnectionString(String connectionString) {
            if (connectionString == null || connectionString.isEmpty()) {
                throw new IllegalArgumentException("connectionString must not be null or empty");
            }
            this.connectionString = connectionString;
            this.blobServiceEndpoint = null;
            this.credential = null;
            this.blobServiceClient = null;
            return this;
        }

        /**
         * Sets the Azure Blob Storage service endpoint. Use with {@link #setCredential(TokenCredential)}.
         * Mutually exclusive with {@link #setConnectionString(String)} and {@link #setBlobServiceClient(BlobServiceClient)}.
         * Setting this clears any previously set connection string or pre-configured client.
         *
         * @param blobServiceEndpoint the blob service endpoint URL
         * @return this builder
         */
        public Builder setBlobServiceEndpoint(String blobServiceEndpoint) {
            if (blobServiceEndpoint == null || blobServiceEndpoint.isEmpty()) {
                throw new IllegalArgumentException("blobServiceEndpoint must not be null or empty");
            }
            this.blobServiceEndpoint = blobServiceEndpoint;
            this.connectionString = null;
            this.blobServiceClient = null;
            return this;
        }

        /**
         * Sets the token credential for authenticating to Azure Blob Storage.
         * Used with {@link #setBlobServiceEndpoint(String)}.
         *
         * @param credential the token credential
         * @return this builder
         */
        public Builder setCredential(TokenCredential credential) {
            if (credential == null) {
                throw new IllegalArgumentException("credential must not be null");
            }
            this.credential = credential;
            return this;
        }

        /**
         * Sets a pre-configured BlobServiceClient. Mutually exclusive with
         * {@link #setConnectionString(String)} and {@link #setBlobServiceEndpoint(String)}.
         * Setting this clears any previously set connection string or endpoint.
         *
         * @param blobServiceClient the pre-configured client
         * @return this builder
         */
        public Builder setBlobServiceClient(BlobServiceClient blobServiceClient) {
            if (blobServiceClient == null) {
                throw new IllegalArgumentException("blobServiceClient must not be null");
            }
            this.blobServiceClient = blobServiceClient;
            this.connectionString = null;
            this.blobServiceEndpoint = null;
            this.credential = null;
            return this;
        }

        /**
         * Sets the container name. Defaults to {@value DEFAULT_CONTAINER_NAME}.
         *
         * @param containerName the container name
         * @return this builder
         */
        public Builder setContainerName(String containerName) {
            if (containerName == null || containerName.isEmpty()) {
                throw new IllegalArgumentException("containerName must not be null or empty");
            }
            this.containerName = containerName;
            return this;
        }

        /**
         * Sets the blob name prefix. Defaults to {@value DEFAULT_BLOB_PREFIX}.
         *
         * @param blobPrefix the blob name prefix
         * @return this builder
         */
        public Builder setBlobPrefix(String blobPrefix) {
            if (blobPrefix == null) {
                throw new IllegalArgumentException("blobPrefix must not be null");
            }
            this.blobPrefix = blobPrefix;
            return this;
        }

        /**
         * Sets whether payloads should be compressed with gzip before uploading.
         * Defaults to {@code true}.
         *
         * @param compressPayloads true to enable gzip compression
         * @return this builder
         */
        public Builder setCompressPayloads(boolean compressPayloads) {
            this.compressPayloads = compressPayloads;
            return this;
        }

        /**
         * Builds a new {@link BlobPayloadStoreOptions} instance.
         *
         * @return the options instance
         * @throws IllegalStateException if no authentication method is configured
         */
        public BlobPayloadStoreOptions build() {
            int authMethods = 0;
            if (connectionString != null) authMethods++;
            if (blobServiceEndpoint != null) authMethods++;
            if (blobServiceClient != null) authMethods++;

            if (authMethods == 0) {
                throw new IllegalStateException(
                    "One of connectionString, blobServiceEndpoint, or blobServiceClient must be set");
            }
            if (authMethods > 1) {
                throw new IllegalStateException(
                    "Only one of connectionString, blobServiceEndpoint, or blobServiceClient may be set");
            }
            if (blobServiceEndpoint != null && credential == null) {
                throw new IllegalStateException(
                    "credential must be set when using blobServiceEndpoint");
            }
            return new BlobPayloadStoreOptions(this);
        }
    }
}
