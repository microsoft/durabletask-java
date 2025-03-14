// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.shared.azuremanaged;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Parses and manages connection strings for Azure-managed Durable Task Scheduler.
 * This class is used by both client and worker components.
 */
public class DurableTaskSchedulerConnectionString {
    private final String endpoint;
    private final String taskHubName;
    private final String resourceId;
    private final boolean allowInsecure;

    /**
     * Creates a new instance of DurableTaskSchedulerConnectionString.
     * 
     * @param endpoint The endpoint URL of the Durable Task Scheduler service.
     * @param taskHubName The name of the task hub.
     * @param resourceId The Azure resource ID for authentication.
     * @param allowInsecure Whether to allow insecure connections.
     */
    public DurableTaskSchedulerConnectionString(
            String endpoint,
            String taskHubName,
            String resourceId,
            boolean allowInsecure) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        this.taskHubName = Objects.requireNonNull(taskHubName, "taskHubName must not be null");
        this.resourceId = resourceId != null ? resourceId : "https://durabletask.io";
        this.allowInsecure = allowInsecure;
    }

    /**
     * Parses a connection string into a DurableTaskSchedulerConnectionString object.
     * 
     * @param connectionString The connection string to parse.
     * @return A new DurableTaskSchedulerConnectionString object.
     * @throws IllegalArgumentException If the connection string is invalid.
     */
    public static DurableTaskSchedulerConnectionString parse(String connectionString) {
        if (connectionString == null || connectionString.isEmpty()) {
            throw new IllegalArgumentException("connectionString must not be null or empty");
        }

        Map<String, String> properties = parseConnectionString(connectionString);
        
        String endpoint = properties.get("Endpoint");
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalArgumentException("The connection string must contain an Endpoint property");
        }

        String taskHubName = properties.get("TaskHubName");
        if (taskHubName == null || taskHubName.isEmpty()) {
            throw new IllegalArgumentException("The connection string must contain a TaskHubName property");
        }

        String resourceId = properties.get("ResourceId");
        
        String allowInsecureStr = properties.get("AllowInsecure");
        boolean allowInsecure = "true".equalsIgnoreCase(allowInsecureStr);

        return new DurableTaskSchedulerConnectionString(endpoint, taskHubName, resourceId, allowInsecure);
    }

    private static Map<String, String> parseConnectionString(String connectionString) {
        Map<String, String> properties = new HashMap<>();
        
        String[] pairs = connectionString.split(";");
        for (String pair : pairs) {
            int equalsIndex = pair.indexOf('=');
            if (equalsIndex > 0) {
                String key = pair.substring(0, equalsIndex).trim();
                String value = pair.substring(equalsIndex + 1).trim();
                properties.put(key, value);
            }
        }
        
        return properties;
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
     * Gets the task hub name.
     * 
     * @return The task hub name.
     */
    public String getTaskHubName() {
        return taskHubName;
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
     * Gets whether insecure connections are allowed.
     * 
     * @return True if insecure connections are allowed, false otherwise.
     */
    public boolean isAllowInsecure() {
        return allowInsecure;
    }
} 