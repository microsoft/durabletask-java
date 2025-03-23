// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.azure.core.credential.TokenCredential;
import io.grpc.Channel;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Extension methods for creating DurableTaskClient instances that connect to Azure-managed Durable Task Scheduler.
 * This class provides various methods to create and configure clients using either connection strings or explicit parameters.
 */
public final class DurableTaskSchedulerClientExtensions {

    private DurableTaskSchedulerClientExtensions() {}

    /**
     * Configures a DurableTaskGrpcClientBuilder to use Azure-managed Durable Task Scheduler with a connection string.
     * 
     * @param builder The builder to configure.
     * @param connectionString The connection string for Azure-managed Durable Task Scheduler.
     * @throws NullPointerException if builder or connectionString is null
     */
    public static void useDurableTaskScheduler(
            DurableTaskGrpcClientBuilder builder,
            String connectionString) {
        Objects.requireNonNull(builder, "builder must not be null");
        Objects.requireNonNull(connectionString, "connectionString must not be null");
        
        configureBuilder(builder, 
            DurableTaskSchedulerClientOptions.fromConnectionString(connectionString));
    }

    /**
     * Configures a DurableTaskGrpcClientBuilder to use Azure-managed Durable Task Scheduler with explicit parameters.
     * 
     * @param builder The builder to configure.
     * @param endpoint The endpoint address for Azure-managed Durable Task Scheduler.
     * @param taskHubName The name of the task hub to connect to.
     * @param tokenCredential The token credential for authentication, or null for anonymous access.
     * @throws NullPointerException if builder, endpoint, or taskHubName is null
     */
    public static void useDurableTaskScheduler(
            DurableTaskGrpcClientBuilder builder,
            String endpoint,
            String taskHubName,
            @Nullable TokenCredential tokenCredential) {
        Objects.requireNonNull(builder, "builder must not be null");
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");
        
        configureBuilder(builder, new DurableTaskSchedulerClientOptions()
            .setEndpointAddress(endpoint)
            .setTaskHubName(taskHubName)
            .setCredential(tokenCredential));
    }

    /**
     * Creates a DurableTaskGrpcClientBuilder configured for Azure-managed Durable Task Scheduler using a connection string.
     * 
     * @param connectionString The connection string for Azure-managed Durable Task Scheduler.
     * @return A new configured DurableTaskGrpcClientBuilder instance.
     * @throws NullPointerException if connectionString is null
     */
    public static DurableTaskGrpcClientBuilder createClientBuilder(
            String connectionString) {
        Objects.requireNonNull(connectionString, "connectionString must not be null");
        return createBuilderFromOptions(
            DurableTaskSchedulerClientOptions.fromConnectionString(connectionString));
    }

    /**
     * Creates a DurableTaskGrpcClientBuilder configured for Azure-managed Durable Task Scheduler using explicit parameters.
     * 
     * @param endpoint The endpoint address for Azure-managed Durable Task Scheduler.
     * @param taskHubName The name of the task hub to connect to.
     * @param tokenCredential The token credential for authentication, or null for anonymous access.
     * @return A new configured DurableTaskGrpcClientBuilder instance.
     * @throws NullPointerException if endpoint or taskHubName is null
     */
    public static DurableTaskGrpcClientBuilder createClientBuilder(
            String endpoint,
            String taskHubName,
            @Nullable TokenCredential tokenCredential) {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");
        
        return createBuilderFromOptions(new DurableTaskSchedulerClientOptions()
            .setEndpointAddress(endpoint)
            .setTaskHubName(taskHubName)
            .setCredential(tokenCredential));
    }

    // Private helper methods to reduce code duplication
    private static DurableTaskGrpcClientBuilder createBuilderFromOptions(DurableTaskSchedulerClientOptions options) {
        Channel grpcChannel = options.createGrpcChannel();
        return new DurableTaskGrpcClientBuilder().grpcChannel(grpcChannel);
    }

    private static void configureBuilder(DurableTaskGrpcClientBuilder builder, DurableTaskSchedulerClientOptions options) {
        Channel grpcChannel = options.createGrpcChannel();
        builder.grpcChannel(grpcChannel);
    }
} 