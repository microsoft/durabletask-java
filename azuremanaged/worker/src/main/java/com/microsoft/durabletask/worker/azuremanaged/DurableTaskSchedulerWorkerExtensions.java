// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.worker.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.microsoft.durabletask.DurableTaskGrpcWorker;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
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
import javax.annotation.Nullable;

/**
 * Extension methods for creating DurableTaskWorker instances that connect to Azure-managed Durable Task Scheduler.
 * This class provides various methods to create and configure workers using either connection strings or explicit parameters.
 */
public static class DurableTaskSchedulerWorkerExtensions {
    /**
     * Creates a DurableTaskWorker using a connection string.
     * 
     * @param connectionString The connection string for Azure-managed Durable Task Scheduler.
     * @return A new DurableTaskWorker instance.
     */
    public static DurableTaskWorker createWorker(String connectionString) {
        return createWorker(connectionString, null);
    }

    /**
     * Creates a DurableTaskWorker using a connection string and token credential.
     * 
     * @param connectionString The connection string for Azure-managed Durable Task Scheduler.
     * @param tokenCredential The token credential for authentication, or null to use connection string credentials.
     * @return A new DurableTaskWorker instance.
     * @throws NullPointerException if connectionString is null
     */
    public static DurableTaskWorker createWorker(String connectionString, @Nullable TokenCredential tokenCredential) {
        Objects.requireNonNull(connectionString, "connectionString must not be null");
        return createWorkerFromOptions(
            DurableTaskSchedulerWorkerOptions.fromConnectionString(connectionString, tokenCredential));
    }

    /**
     * Creates a DurableTaskWorker using explicit endpoint and task hub parameters.
     * 
     * @param endpoint The endpoint address for Azure-managed Durable Task Scheduler.
     * @param taskHubName The name of the task hub to connect to.
     * @param tokenCredential The token credential for authentication, or null for anonymous access.
     * @return A new DurableTaskWorker instance.
     * @throws NullPointerException if endpoint or taskHubName is null
     */
    public static DurableTaskWorker createWorker(
            String endpoint,
            String taskHubName,
            @Nullable TokenCredential tokenCredential) {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");
        
        return createWorkerFromOptions(new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress(endpoint)
            .setTaskHubName(taskHubName)
            .setCredential(tokenCredential));
    }

    /**
     * Configures a DurableTaskGrpcWorkerBuilder to use Azure-managed Durable Task Scheduler with a connection string.
     * 
     * @param builder The builder to configure.
     * @param connectionString The connection string for Azure-managed Durable Task Scheduler.
     * @param tokenCredential The token credential for authentication, or null to use connection string credentials.
     * @throws NullPointerException if builder or connectionString is null
     */
    public static void useDurableTaskScheduler(
            DurableTaskGrpcWorkerBuilder builder,
            String connectionString,
            @Nullable TokenCredential tokenCredential) {
        Objects.requireNonNull(builder, "builder must not be null");
        Objects.requireNonNull(connectionString, "connectionString must not be null");
        
        configureBuilder(builder, 
            DurableTaskSchedulerWorkerOptions.fromConnectionString(connectionString, tokenCredential));
    }

    /**
     * Configures a DurableTaskGrpcWorkerBuilder to use Azure-managed Durable Task Scheduler with explicit parameters.
     * 
     * @param builder The builder to configure.
     * @param endpoint The endpoint address for Azure-managed Durable Task Scheduler.
     * @param taskHubName The name of the task hub to connect to.
     * @param tokenCredential The token credential for authentication, or null for anonymous access.
     * @throws NullPointerException if builder, endpoint, or taskHubName is null
     */
    public static void useDurableTaskScheduler(
            DurableTaskGrpcWorkerBuilder builder,
            String endpoint,
            String taskHubName,
            @Nullable TokenCredential tokenCredential) {
        Objects.requireNonNull(builder, "builder must not be null");
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");
        
        configureBuilder(builder, new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress(endpoint)
            .setTaskHubName(taskHubName)
            .setCredential(tokenCredential));
    }

    /**
     * Creates a DurableTaskGrpcWorkerBuilder configured for Azure-managed Durable Task Scheduler using a connection string.
     * 
     * @param connectionString The connection string for Azure-managed Durable Task Scheduler.
     * @param tokenCredential The token credential for authentication, or null to use connection string credentials.
     * @return A new configured DurableTaskGrpcWorkerBuilder instance.
     * @throws NullPointerException if connectionString is null
     */
    public static DurableTaskGrpcWorkerBuilder createWorkerBuilder(
            String connectionString,
            @Nullable TokenCredential tokenCredential) {
        Objects.requireNonNull(connectionString, "connectionString must not be null");
        return createBuilderFromOptions(
            DurableTaskSchedulerWorkerOptions.fromConnectionString(connectionString, tokenCredential));
    }

    /**
     * Creates a DurableTaskGrpcWorkerBuilder configured for Azure-managed Durable Task Scheduler using explicit parameters.
     * 
     * @param endpoint The endpoint address for Azure-managed Durable Task Scheduler.
     * @param taskHubName The name of the task hub to connect to.
     * @param tokenCredential The token credential for authentication, or null for anonymous access.
     * @return A new configured DurableTaskGrpcWorkerBuilder instance.
     * @throws NullPointerException if endpoint or taskHubName is null
     */
    public static DurableTaskGrpcWorkerBuilder createWorkerBuilder(
            String endpoint,
            String taskHubName,
            @Nullable TokenCredential tokenCredential) {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");
        
        return createBuilderFromOptions(new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress(endpoint)
            .setTaskHubName(taskHubName)
            .setCredential(tokenCredential));
    }

    // Private helper methods to reduce code duplication

    private static DurableTaskWorker createWorkerFromOptions(DurableTaskSchedulerWorkerOptions options) {
        return createBuilderFromOptions(options).build();
    }

    private static DurableTaskGrpcWorkerBuilder createBuilderFromOptions(DurableTaskSchedulerWorkerOptions options) {
        Channel grpcChannel = options.createGrpcChannel();
        return new DurableTaskGrpcWorkerBuilder().grpcChannel(grpcChannel);
    }

    private static void configureBuilder(DurableTaskGrpcWorkerBuilder builder, DurableTaskSchedulerWorkerOptions options) {
        Channel grpcChannel = options.createGrpcChannel();
        builder.grpcChannel(grpcChannel);
    }
} 