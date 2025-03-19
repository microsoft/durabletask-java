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
     * Creates a DurableTaskClient that connects to Azure-managed Durable Task Scheduler using a connection string.
     * 
     * @param connectionString The connection string for connecting to Azure-managed Durable Task Scheduler.
     * @param tokenCredential The token credential for connecting to Azure-managed Durable Task Scheduler.
     * @return A new DurableTaskClient instance.
     */
    public static DurableTaskClient createClient(String connectionString, @Nullable TokenCredential tokenCredential) {
        Objects.requireNonNull(connectionString, "connectionString must not be null");
        DurableTaskSchedulerClientOptions options = DurableTaskSchedulerClientOptions.fromConnectionString(connectionString, tokenCredential);
        Channel grpcChannel = options.createGrpcChannel();

        return new DurableTaskGrpcClientBuilder()
            .grpcChannel(grpcChannel)
            .build();
    }

    public static DurableTaskClient createClient(String endpoint, String taskHubName, @Nullable TokenCredential tokenCredential) {
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions()
            .setEndpoint(endpoint)
            .setTaskHubName(taskHubName)
            .setTokenCredential(tokenCredential);

        Channel grpcChannel = options.createGrpcChannel();

        return new DurableTaskGrpcClientBuilder()
            .grpcChannel(grpcChannel)
            .build();
    }

    public static void UseDurableTaskScheduler(DurableTaskGrpcClientBuilder builder, String endpoint, String taskHubName, @Nullable TokenCredential tokenCredential) {
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions()
            .setEndpoint(endpoint)
            .setTaskHubName(taskHubName)
            .setTokenCredential(tokenCredential);

        Channel grpcChannel = options.createGrpcChannel();
        builder.grpcChannel(grpcChannel);
    }
    public static void UseDurableTaskScheduler(DurableTaskGrpcClientBuilder builder, String connectionString, @Nullable TokenCredential tokenCredential) {
        DurableTaskSchedulerClientOptions options = DurableTaskSchedulerClientOptions.fromConnectionString(connectionString, tokenCredential);
        Channel grpcChannel = options.createGrpcChannel();
        builder.grpcChannel(grpcChannel);
    }

    /**
     * Creates a DurableTaskGrpcClientBuilder that connects to Azure-managed Durable Task Scheduler using a connection string.
     * 
     * @param connectionString The connection string for connecting to Azure-managed Durable Task Scheduler.
     * @param tokenCredential The token credential for connecting to Azure-managed Durable Task Scheduler.
     * @return A new DurableTaskGrpcClientBuilder instance.
     */
    public static DurableTaskGrpcClientBuilder createClientBuilder(String connectionString, @Nullable TokenCredential tokenCredential) {
        Objects.requireNonNull(connectionString, "connectionString must not be null");
        DurableTaskSchedulerClientOptions options = DurableTaskSchedulerClientOptions.fromConnectionString(connectionString, tokenCredential);
        Channel grpcChannel = options.createGrpcChannel();

        return new DurableTaskGrpcClientBuilder()
            .grpcChannel(grpcChannel);
    }

    /**
     * Creates a DurableTaskGrpcClientBuilder that connects to Azure-managed Durable Task Scheduler.
     * 
     * @param endpoint The endpoint for connecting to Azure-managed Durable Task Scheduler.
     * @param taskHubName The task hub name for connecting to Azure-managed Durable Task Scheduler.
     * @param tokenCredential The token credential for connecting to Azure-managed Durable Task Scheduler.
     * @return A new DurableTaskGrpcClientBuilder instance.
     */
    public static DurableTaskGrpcClientBuilder createClientBuilder(String endpoint, String taskHubName, @Nullable TokenCredential tokenCredential) {
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions()
            .setEndpoint(endpoint)
            .setTaskHubName(taskHubName)
            .setTokenCredential(tokenCredential);

        Channel grpcChannel = options.createGrpcChannel();

        return new DurableTaskGrpcClientBuilder()
            .grpcChannel(grpcChannel);
    }
} 