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

    private static Channel createGrpcChannel(DurableTaskSchedulerClientOptions options, AccessTokenCache tokenCache) {
        // Normalize the endpoint URL and add DNS scheme for gRPC name resolution
        String endpoint = "dns:///" + options.getEndpoint();

        // Create metadata interceptor to add task hub name and auth token
        ClientInterceptor metadataInterceptor = new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                        next.newCall(method, callOptions)) {
                    @Override
                    public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
                        headers.put(
                            Metadata.Key.of("taskhub", Metadata.ASCII_STRING_MARSHALLER),
                            options.getTaskHubName()
                        );
                        
                        // Add authorization token if credentials are configured
                        if (tokenCache != null) {
                            String token = tokenCache.getToken().getToken();
                            headers.put(
                                Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER),
                                "Bearer " + token
                            );
                        }
                        
                        super.start(responseListener, headers);
                    }
                };
            }
        };
        
        // Build the channel with appropriate security settings
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(endpoint)
            .intercept(metadataInterceptor);
            
        if (!options.isAllowInsecure()) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }

        return builder.build();
    }
} 