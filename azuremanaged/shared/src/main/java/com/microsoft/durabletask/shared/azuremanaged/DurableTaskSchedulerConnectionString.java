// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.shared.azuremanaged;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the constituent parts of a connection string for a Durable Task Scheduler service.
 */
public class DurableTaskSchedulerConnectionString {
    private final Map<String, String> properties;

    /**
     * Initializes a new instance of the DurableTaskSchedulerConnectionString class.
     * 
     * @param connectionString A connection string for a Durable Task Scheduler service.
     * @throws IllegalArgumentException If the connection string is invalid or missing required properties.
     */
    public DurableTaskSchedulerConnectionString(String connectionString) {
        if (connectionString == null || connectionString.trim().isEmpty()) {
            throw new IllegalArgumentException("connectionString must not be null or empty");
        }
        this.properties = parseConnectionString(connectionString);
        
        // Validate required properties
        getRequiredValue("Endpoint");
        getRequiredValue("Authentication");
        getRequiredValue("TaskHub");
    }

    /**
     * Gets the authentication method specified in the connection string.
     * 
     * @return The authentication method.
     */
    public String getAuthentication() {
        return getRequiredValue("Authentication");
    }

    /**
     * Gets the managed identity or workload identity client ID specified in the connection string.
     * 
     * @return The client ID, or null if not specified.
     */
    public String getClientId() {
        return getValue("ClientID");
    }

    /**
     * Gets the "AdditionallyAllowedTenants" property, optionally used by Workload Identity.
     * Multiple values can be separated by a comma.
     * 
     * @return List of allowed tenants, or null if not specified.
     */
    public List<String> getAdditionallyAllowedTenants() {
        String value = getValue("AdditionallyAllowedTenants");
        if (value == null || value.isEmpty()) {
            return null;
        }
        return Arrays.asList(value.split(","));
    }

    /**
     * Gets the "TenantId" property, optionally used by Workload Identity.
     * 
     * @return The tenant ID, or null if not specified.
     */
    public String getTenantId() {
        return getValue("TenantId");
    }

    /**
     * Gets the "TokenFilePath" property, optionally used by Workload Identity.
     * 
     * @return The token file path, or null if not specified.
     */
    public String getTokenFilePath() {
        return getValue("TokenFilePath");
    }

    /**
     * Gets the endpoint specified in the connection string.
     * 
     * @return The endpoint URL.
     */
    public String getEndpoint() {
        return getRequiredValue("Endpoint");
    }

    /**
     * Gets the task hub name specified in the connection string.
     * 
     * @return The task hub name.
     */
    public String getTaskHubName() {
        return getRequiredValue("TaskHub");
    }

    private String getValue(String name) {
        return properties.get(name);
    }

    private String getRequiredValue(String name) {
        String value = getValue(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("The connection string must contain a " + name + " property");
        }
        return value;
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
} 