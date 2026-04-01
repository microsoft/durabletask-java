// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.LargePayloadOptions;

/**
 * Extension methods for configuring Azure Blob Storage-based payload externalization
 * on Durable Task builders.
 * <p>
 * Example:
 * <pre>{@code
 * BlobPayloadStoreOptions storeOptions = new BlobPayloadStoreOptions.Builder()
 *     .setConnectionString("DefaultEndpointsProtocol=https;...")
 *     .build();
 *
 * DurableTaskGrpcWorkerBuilder workerBuilder = new DurableTaskGrpcWorkerBuilder();
 * AzureBlobPayloadsExtensions.useBlobStoragePayloads(workerBuilder, storeOptions);
 *
 * DurableTaskGrpcClientBuilder clientBuilder = new DurableTaskGrpcClientBuilder();
 * AzureBlobPayloadsExtensions.useBlobStoragePayloads(clientBuilder, storeOptions);
 * }</pre>
 *
 * @see BlobPayloadStore
 * @see BlobPayloadStoreOptions
 */
public final class AzureBlobPayloadsExtensions {

    private AzureBlobPayloadsExtensions() {
    }

    /**
     * Configures the worker builder to use Azure Blob Storage for large payload externalization
     * with default {@link LargePayloadOptions}.
     *
     * @param builder      the worker builder to configure
     * @param storeOptions the blob payload store configuration
     */
    public static void useBlobStoragePayloads(
            DurableTaskGrpcWorkerBuilder builder,
            BlobPayloadStoreOptions storeOptions) {
        if (builder == null) {
            throw new IllegalArgumentException("builder must not be null");
        }
        if (storeOptions == null) {
            throw new IllegalArgumentException("storeOptions must not be null");
        }
        builder.useExternalizedPayloads(new BlobPayloadStore(storeOptions));
    }

    /**
     * Configures the worker builder to use Azure Blob Storage for large payload externalization
     * with custom {@link LargePayloadOptions}.
     *
     * @param builder      the worker builder to configure
     * @param storeOptions the blob payload store configuration
     * @param payloadOptions the large payload threshold options
     */
    public static void useBlobStoragePayloads(
            DurableTaskGrpcWorkerBuilder builder,
            BlobPayloadStoreOptions storeOptions,
            LargePayloadOptions payloadOptions) {
        if (builder == null) {
            throw new IllegalArgumentException("builder must not be null");
        }
        if (storeOptions == null) {
            throw new IllegalArgumentException("storeOptions must not be null");
        }
        builder.useExternalizedPayloads(new BlobPayloadStore(storeOptions), payloadOptions);
    }

    /**
     * Configures the client builder to use Azure Blob Storage for large payload externalization
     * with default {@link LargePayloadOptions}.
     *
     * @param builder      the client builder to configure
     * @param storeOptions the blob payload store configuration
     */
    public static void useBlobStoragePayloads(
            DurableTaskGrpcClientBuilder builder,
            BlobPayloadStoreOptions storeOptions) {
        if (builder == null) {
            throw new IllegalArgumentException("builder must not be null");
        }
        if (storeOptions == null) {
            throw new IllegalArgumentException("storeOptions must not be null");
        }
        builder.useExternalizedPayloads(new BlobPayloadStore(storeOptions));
    }

    /**
     * Configures the client builder to use Azure Blob Storage for large payload externalization
     * with custom {@link LargePayloadOptions}.
     *
     * @param builder      the client builder to configure
     * @param storeOptions the blob payload store configuration
     * @param payloadOptions the large payload threshold options
     */
    public static void useBlobStoragePayloads(
            DurableTaskGrpcClientBuilder builder,
            BlobPayloadStoreOptions storeOptions,
            LargePayloadOptions payloadOptions) {
        if (builder == null) {
            throw new IllegalArgumentException("builder must not be null");
        }
        if (storeOptions == null) {
            throw new IllegalArgumentException("storeOptions must not be null");
        }
        builder.useExternalizedPayloads(new BlobPayloadStore(storeOptions), payloadOptions);
    }
}
