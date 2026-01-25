// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import io.grpc.Channel;
import io.grpc.ClientInterceptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builder class for constructing new {@link DurableTaskClient} objects that communicate with a sidecar process
 * over gRPC.
 */
public final class DurableTaskGrpcClientBuilder {
    DataConverter dataConverter;
    int port;
    Channel channel;
    String defaultVersion;
    List<ClientInterceptor> interceptors = new ArrayList<>();

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
     * Adds a gRPC client interceptor to be applied to all gRPC calls made by the client.
     * <p>
     * Interceptors can be used to add custom headers, logging, or other cross-cutting concerns
     * to gRPC calls. Multiple interceptors can be added and will be applied in the order they
     * were added.
     *
     * @param interceptor the gRPC client interceptor to add
     * @return this builder object
     */
    public DurableTaskGrpcClientBuilder addInterceptor(ClientInterceptor interceptor) {
        if (interceptor != null) {
            this.interceptors.add(interceptor);
        }
        return this;
    }

    /**
     * Gets the list of interceptors that have been added to this builder.
     *
     * @return an unmodifiable list of interceptors
     */
    List<ClientInterceptor> getInterceptors() {
        return Collections.unmodifiableList(this.interceptors);
    }

    /**
     * Initializes a new {@link DurableTaskClient} object with the settings specified in the current builder object.
     * @return a new {@link DurableTaskClient} object
     */
    public DurableTaskClient build() {
        return new DurableTaskGrpcClient(this);
    }
}
