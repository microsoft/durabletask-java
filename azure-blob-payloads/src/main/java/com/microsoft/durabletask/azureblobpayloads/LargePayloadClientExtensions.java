// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;

/**
 * Extension methods for enabling externalized payload storage on a {@link DurableTaskGrpcClientBuilder}.
 * <p>
 * This mirrors the .NET {@code UseExternalizedPayloads()} extension method pattern.
 */
public final class LargePayloadClientExtensions {

    private LargePayloadClientExtensions() {
    }

    /**
     * Enables externalized payload storage using Azure Blob Storage for the specified client builder.
     * <p>
     * Creates a new {@link BlobPayloadStore} from the given options and registers the
     * {@link LargePayloadInterceptor} on the builder's gRPC channel.
     *
     * @param builder the client builder to configure
     * @param options the storage options
     * @return the builder, for call chaining
     */
    public static DurableTaskGrpcClientBuilder useExternalizedPayloads(
            DurableTaskGrpcClientBuilder builder,
            LargePayloadStorageOptions options) {
        PayloadStore store = new BlobPayloadStore(options);
        builder.addInterceptor(new LargePayloadInterceptor(store, options));
        return builder;
    }

    /**
     * Enables externalized payload storage using a pre-configured shared payload store
     * for the specified client builder.
     * <p>
     * This overload ensures the client and worker share the same {@link PayloadStore} instance.
     *
     * @param builder the client builder to configure
     * @param store the shared payload store
     * @param options the storage options (used for threshold/max-size configuration)
     * @return the builder, for call chaining
     */
    public static DurableTaskGrpcClientBuilder useExternalizedPayloads(
            DurableTaskGrpcClientBuilder builder,
            PayloadStore store,
            LargePayloadStorageOptions options) {
        builder.addInterceptor(new LargePayloadInterceptor(store, options));
        return builder;
    }
}
