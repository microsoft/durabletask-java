// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.worker.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.microsoft.durabletask.shared.azuremanaged.DurableTaskSchedulerConnectionString;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration options for connecting to Azure-managed Durable Task Scheduler as a worker.
 */
public class DurableTaskSchedulerWorkerOptions {
    private String endpoint;
    private String taskHubName;
    private String resourceId = "https://durabletask.io";
    private boolean allowInsecure = false;
    private TokenCredential tokenCredential;
    private Duration tokenRefreshMargin = Duration.ofMinutes(5);

    /**
     * Creates a new instance of DurableTaskSchedulerWorkerOptions.
     */
    public DurableTaskSchedulerWorkerOptions() {
    }

    /**
     * Creates a new instance of DurableTaskSchedulerWorkerOptions from a connection string.
     * 
     * @param connectionString The connection string to parse.
     * @return A new DurableTaskSchedulerWorkerOptions object.
     */
    public static DurableTaskSchedulerWorkerOptions fromConnectionString(String connectionString) {
        DurableTaskSchedulerConnectionString parsedConnectionString = DurableTaskSchedulerConnectionString.parse(connectionString);
        
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions();
        options.setEndpoint(parsedConnectionString.getEndpoint());
        options.setTaskHubName(parsedConnectionString.getTaskHubName());
        options.setResourceId(parsedConnectionString.getResourceId());
        options.setAllowInsecure(parsedConnectionString.isAllowInsecure());
        
        return options;
    }

    /**
     * Gets the endpoint URL.
     * 
     * @return The endpoint URL.
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Sets the endpoint URL.
     * 
     * @param endpoint The endpoint URL.
     * @return This options object.
     */
    public DurableTaskSchedulerWorkerOptions setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    /**
     * Gets the task hub name.
     * 
     * @return The task hub name.
     */
    public String getTaskHubName() {
        return taskHubName;
    }

    /**
     * Sets the task hub name.
     * 
     * @param taskHubName The task hub name.
     * @return This options object.
     */
    public DurableTaskSchedulerWorkerOptions setTaskHubName(String taskHubName) {
        this.taskHubName = taskHubName;
        return this;
    }

    /**
     * Gets the resource ID for authentication.
     * 
     * @return The resource ID.
     */
    public String getResourceId() {
        return resourceId;
    }

    /**
     * Sets the resource ID for authentication.
     * 
     * @param resourceId The resource ID.
     * @return This options object.
     */
    public DurableTaskSchedulerWorkerOptions setResourceId(String resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    /**
     * Gets whether insecure connections are allowed.
     * 
     * @return True if insecure connections are allowed, false otherwise.
     */
    public boolean isAllowInsecure() {
        return allowInsecure;
    }

    /**
     * Sets whether insecure connections are allowed.
     * 
     * @param allowInsecure True to allow insecure connections, false otherwise.
     * @return This options object.
     */
    public DurableTaskSchedulerWorkerOptions setAllowInsecure(boolean allowInsecure) {
        this.allowInsecure = allowInsecure;
        return this;
    }

    /**
     * Gets the token credential for authentication.
     * 
     * @return The token credential.
     */
    public TokenCredential getTokenCredential() {
        return tokenCredential;
    }

    /**
     * Sets the token credential for authentication.
     * 
     * @param tokenCredential The token credential.
     * @return This options object.
     */
    public DurableTaskSchedulerWorkerOptions setTokenCredential(TokenCredential tokenCredential) {
        this.tokenCredential = tokenCredential;
        return this;
    }

    /**
     * Gets the token refresh margin.
     * 
     * @return The token refresh margin.
     */
    public Duration getTokenRefreshMargin() {
        return tokenRefreshMargin;
    }

    /**
     * Sets the token refresh margin.
     * 
     * @param tokenRefreshMargin The token refresh margin.
     * @return This options object.
     */
    public DurableTaskSchedulerWorkerOptions setTokenRefreshMargin(Duration tokenRefreshMargin) {
        this.tokenRefreshMargin = tokenRefreshMargin;
        return this;
    }

    /**
     * Validates that the options are properly configured.
     * 
     * @throws IllegalArgumentException If the options are not properly configured.
     */
    public void validate() {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");
    }
} 