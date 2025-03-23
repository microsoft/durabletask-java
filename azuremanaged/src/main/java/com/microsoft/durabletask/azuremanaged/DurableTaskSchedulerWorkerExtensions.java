// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;

import io.grpc.Channel;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Extension methods for creating DurableTaskWorker instances that connect to Azure-managed Durable Task Scheduler.
 * This class provides various methods to create and configure workers using either connection strings or explicit parameters.
 */
public final class DurableTaskSchedulerWorkerExtensions {
    private DurableTaskSchedulerWorkerExtensions() {}

    /**
     * Configures a DurableTaskGrpcWorkerBuilder to use Azure-managed Durable Task Scheduler with a connection string.
     * 
     * @param builder The builder to configure.
     * @param connectionString The connection string for Azure-managed Durable Task Scheduler.
     * @throws NullPointerException if builder or connectionString is null
     */
    public static void useDurableTaskScheduler(
            DurableTaskGrpcWorkerBuilder builder,
            String connectionString) {
        Objects.requireNonNull(builder, "builder must not be null");
        Objects.requireNonNull(connectionString, "connectionString must not be null");
        
        configureBuilder(builder, 
            DurableTaskSchedulerWorkerOptions.fromConnectionString(connectionString));
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
     * @return A new configured DurableTaskGrpcWorkerBuilder instance.
     * @throws NullPointerException if connectionString is null
     */
    public static DurableTaskGrpcWorkerBuilder createWorkerBuilder(
            String connectionString) {
        Objects.requireNonNull(connectionString, "connectionString must not be null");
        return createBuilderFromOptions(
            DurableTaskSchedulerWorkerOptions.fromConnectionString(connectionString));
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
    private static DurableTaskGrpcWorkerBuilder createBuilderFromOptions(DurableTaskSchedulerWorkerOptions options) {
        Channel grpcChannel = options.createGrpcChannel();
        return new DurableTaskGrpcWorkerBuilder().grpcChannel(grpcChannel);
    }

    private static void configureBuilder(DurableTaskGrpcWorkerBuilder builder, DurableTaskSchedulerWorkerOptions options) {
        Channel grpcChannel = options.createGrpcChannel();
        builder.grpcChannel(grpcChannel);
    }
} 