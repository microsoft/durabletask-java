// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.client.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.shared.azuremanaged.AccessTokenCache;

import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.CallOptions;

import java.util.Objects;

/**
 * Extension methods for creating DurableTaskClient instances that connect to Azure-managed Durable Task Scheduler.
 */
public class DurableTaskSchedulerClientExtensions {

    /**
     * Creates a DurableTaskClient that connects to Azure-managed Durable Task Scheduler.
     * 
     * @param options The options for connecting to Azure-managed Durable Task Scheduler.
     * @return A new DurableTaskClient instance.
     */
    public static DurableTaskClient createClient(DurableTaskSchedulerClientOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        options.validate();

        // Create the access token cache if credentials are provided
        AccessTokenCache tokenCache = null;
        TokenCredential credential = options.getTokenCredential();
        if (credential != null) {
            TokenRequestContext context = new TokenRequestContext();
            context.addScopes(new String[] { options.getResourceId() + "/.default" });
            tokenCache = new AccessTokenCache(credential, context, options.getTokenRefreshMargin());
        }

        // Create the gRPC channel
        Channel grpcChannel = createGrpcChannel(options, tokenCache);

        // Create and return the client
        return new DurableTaskGrpcClientBuilder()
            .grpcChannel(grpcChannel)
            .build();
    }
} 