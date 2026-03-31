// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import io.grpc.Channel;

/**
 * Builder class for constructing new {@link DurableTaskClient} objects that communicate with a sidecar process
 * over gRPC.
 */
public final class DurableTaskGrpcClientBuilder {
    DataConverter dataConverter;
    int port;
    Channel channel;
    String defaultVersion;
    PayloadStore payloadStore;
    LargePayloadOptions largePayloadOptions;

    /**
     * Sets the {@link DataConverter} to use for converting serializable data payloads.
     *
     * @param dataConverter the {@link DataConverter} to use for converting serializable data payloads
     * @return this builder object
     */
    public DurableTaskGrpcClientBuilder dataConverter(DataConverter dataConverter) {
        this.dataConverter = dataConverter;
        return this;
    }

    /**
     * Sets the gRPC channel to use for communicating with the sidecar process.
     * <p>
     * This builder method allows you to provide your own gRPC channel for communicating with the Durable Task sidecar
     * endpoint. Channels provided using this method won't be closed when the client is closed.
     * Rather, the caller remains responsible for shutting down the channel after disposing the client.
     * <p>
     * If not specified, a gRPC channel will be created automatically for each constructed
     * {@link DurableTaskClient}.
     *
     * @param channel the gRPC channel to use
     * @return this builder object
     */
    public DurableTaskGrpcClientBuilder grpcChannel(Channel channel) {
        this.channel = channel;
        return this;
    }

    /**
     * Sets the gRPC endpoint port to connect to. If not specified, the default Durable Task port number will be used.
     *
     * @param port the gRPC endpoint port to connect to
     * @return this builder object
     */
    public DurableTaskGrpcClientBuilder port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Sets the default version that orchestrations will be created with.
     * 
     * @param defaultVersion the default version to create orchestrations with
     * @return this builder object
     */
    public DurableTaskGrpcClientBuilder defaultVersion(String defaultVersion) {
        this.defaultVersion = defaultVersion;
        return this;
    }

    /**
     * Enables large payload externalization with default options.
     * <p>
     * When enabled, payloads exceeding the default threshold will be uploaded to the
     * provided {@link PayloadStore} and replaced with opaque token references.
     *
     * @param payloadStore the store to use for externalizing large payloads
     * @return this builder object
     */
    public DurableTaskGrpcClientBuilder useExternalizedPayloads(PayloadStore payloadStore) {
        return this.useExternalizedPayloads(payloadStore, new LargePayloadOptions.Builder().build());
    }

    /**
     * Enables large payload externalization with custom options.
     * <p>
     * When enabled, payloads exceeding the configured threshold will be uploaded to the
     * provided {@link PayloadStore} and replaced with opaque token references.
     *
     * @param payloadStore the store to use for externalizing large payloads
     * @param options the large payload configuration options
     * @return this builder object
     */
    public DurableTaskGrpcClientBuilder useExternalizedPayloads(PayloadStore payloadStore, LargePayloadOptions options) {
        if (payloadStore == null) {
            throw new IllegalArgumentException("payloadStore must not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("options must not be null");
        }
        this.payloadStore = payloadStore;
        this.largePayloadOptions = options;
        return this;
    }

    /**
     * Initializes a new {@link DurableTaskClient} object with the settings specified in the current builder object.
     * @return a new {@link DurableTaskClient} object
     */
    public DurableTaskClient build() {
        return new DurableTaskGrpcClient(this);
    }
}
