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
        useDurableTaskScheduler(builder, endpoint, taskHubName, tokenCredential, null);
    }

    /**
     * Configures a worker builder with an explicit token audience.
     *
     * @param builder The builder to configure.
     * @param endpoint The service endpoint, independent of the audience and credential authority.
     * @param taskHubName The name of the task hub.
     * @param tokenCredential The credential, with its authority/cloud configured by the caller,
     *                        or null for anonymous access.
     * @param resourceId The token audience URI, or null/empty for the region-based default.
     *                   See {@link DurableTaskSchedulerWorkerOptions#setResourceId(String)}
     *                   for normalization and default selection.
     * @throws NullPointerException if builder, endpoint, or taskHubName is null.
     * @throws IllegalArgumentException if resourceId becomes empty after normalization.
     */
    public static void useDurableTaskScheduler(
            DurableTaskGrpcWorkerBuilder builder,
            String endpoint,
            String taskHubName,
            @Nullable TokenCredential tokenCredential,
            @Nullable String resourceId) {
        Objects.requireNonNull(builder, "builder must not be null");
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");
        
        configureBuilder(builder, new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress(endpoint)
            .setTaskHubName(taskHubName)
            .setResourceId(resourceId)
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
        return createWorkerBuilder(endpoint, taskHubName, tokenCredential, null);
    }

    /**
     * Creates a worker builder with an explicit token audience.
     *
     * @param endpoint The service endpoint, independent of the audience and credential authority.
     * @param taskHubName The name of the task hub.
     * @param tokenCredential The credential, with its authority/cloud configured by the caller,
     *                        or null for anonymous access.
     * @param resourceId The token audience URI, or null/empty for the region-based default.
     *                   See {@link DurableTaskSchedulerWorkerOptions#setResourceId(String)}
     *                   for normalization and default selection.
     * @return A new configured DurableTaskGrpcWorkerBuilder instance.
     * @throws NullPointerException if endpoint or taskHubName is null.
     * @throws IllegalArgumentException if resourceId becomes empty after normalization.
     */
    public static DurableTaskGrpcWorkerBuilder createWorkerBuilder(
            String endpoint,
            String taskHubName,
            @Nullable TokenCredential tokenCredential,
            @Nullable String resourceId) {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");
        
        return createBuilderFromOptions(new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress(endpoint)
            .setTaskHubName(taskHubName)
            .setResourceId(resourceId)
            .setCredential(tokenCredential)
            .setAllowInsecureCredentials(tokenCredential == null));
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