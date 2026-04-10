// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;

/**
 * Extension methods for enabling externalized payload storage on a {@link DurableTaskGrpcWorkerBuilder}.
 * <p>
 * This mirrors the .NET {@code UseExternalizedPayloads()} extension method pattern.
 * In addition to registering the interceptor, this also enables the
 * {@code WORKER_CAPABILITY_LARGE_PAYLOADS} capability announcement.
 */
public final class LargePayloadWorkerExtensions {

    private LargePayloadWorkerExtensions() {
    }

    /**
     * Enables externalized payload storage using Azure Blob Storage for the specified worker builder.
     * <p>
     * Creates a new {@link BlobPayloadStore} from the given options, registers the
     * {@link LargePayloadInterceptor}, and enables the large payload worker capability.
     *
     * @param builder the worker builder to configure
     * @param options the storage options
     * @return the builder, for call chaining
     */
    public static DurableTaskGrpcWorkerBuilder useExternalizedPayloads(
            DurableTaskGrpcWorkerBuilder builder,
            LargePayloadStorageOptions options) {
        PayloadStore store = new BlobPayloadStore(options);
        builder.addInterceptor(new LargePayloadInterceptor(store, options));
        builder.setSupportsLargePayloads(true);
        return builder;
    }

    /**
     * Enables externalized payload storage using a pre-configured shared payload store
     * for the specified worker builder.
     * <p>
     * This overload ensures the client and worker share the same {@link PayloadStore} instance.
     *
     * @param builder the worker builder to configure
     * @param store the shared payload store
     * @param options the storage options (used for threshold/max-size configuration)
     * @return the builder, for call chaining
     */
    public static DurableTaskGrpcWorkerBuilder useExternalizedPayloads(
            DurableTaskGrpcWorkerBuilder builder,
            PayloadStore store,
            LargePayloadStorageOptions options) {
        builder.addInterceptor(new LargePayloadInterceptor(store, options));
        builder.setSupportsLargePayloads(true);
        return builder;
    }
}
