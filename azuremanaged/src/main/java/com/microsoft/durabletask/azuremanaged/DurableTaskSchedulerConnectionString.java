// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.AzurePowerShellCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.EnvironmentCredentialBuilder;
import com.azure.identity.IntelliJCredentialBuilder;
import com.azure.identity.InteractiveBrowserCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.VisualStudioCodeCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;

/**
 * Represents the constituent parts of a connection string for a Durable Task Scheduler service.
 * Supports optional {@code ResourceId} (token audience URI) and {@code AuthorityHost}
 * (credential authority) properties, independent of {@code Endpoint}.
 */
public class DurableTaskSchedulerConnectionString {
    private final Map<String, String> properties;
    private final String resourceId;

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
        this.getAuthentication();
        this.getTaskHubName();
        this.getEndpoint();
        this.resourceId = ResourceId.resolve(getValue("ResourceId"));
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

    /**
     * Gets the normalized token audience URI, not an Azure Resource Manager resource path.
     * Surrounding whitespace, trailing slashes, and one case-insensitive {@code /.default}
     * suffix are removed from explicit values. Null or empty values select
     * {@code https://durabletask.azure.us} when {@code REGION_NAME} starts with
     * {@code usgov} or {@code usdod} (case-insensitively), otherwise {@code https://durabletask.io}.
     * The default is resolved when this connection string is parsed, not at token refresh.
     *
     * @return The normalized resource ID.
     */
    public String getResourceId() {
        return resourceId;
    }

    /**
     * Gets the optional Microsoft Entra authority host for credentials created by this connection string.
     * Applies to DefaultAzure, Environment, WorkloadIdentity, and InteractiveBrowser authentication.
     * When omitted or empty, Azure Identity retains its default behavior, including
     * {@code AZURE_AUTHORITY_HOST} where supported. Managed identity uses its hosting environment's
     * identity endpoint; developer-tool credentials require the tools' own cloud configuration.
     *
     * @return The authority host, or null if not specified. This does not change the endpoint or audience.
     */
    public @Nullable String getAuthorityHost() {
        return getValue("AuthorityHost");
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
                // Preserve whitespace-only ResourceId values so normalization can reject them.
                String rawValue = pair.substring(equalsIndex + 1);
                String value = key.equals("ResourceId") ? rawValue : rawValue.trim();
                properties.put(key, value);
            }
        }
        
        return properties;
    }

    /**
     * Gets a TokenCredential based on the authentication type specified in the connection string.
     * 
     * @return A TokenCredential instance based on the specified authentication type, or null if authentication type is "none".
     * @throws IllegalArgumentException If the connection string contains an unsupported authentication type.
     */
    public @Nullable TokenCredential getCredential() {
        String authType = getAuthentication();
        String authorityHost = getAuthorityHost();
        boolean hasAuthorityHost = authorityHost != null && !authorityHost.isEmpty();
        
        // Parse the supported auth types in a case-insensitive way
        switch (authType.toLowerCase().trim()) {
            case "defaultazure":
                DefaultAzureCredentialBuilder defaultBuilder = new DefaultAzureCredentialBuilder();
                if (hasAuthorityHost) {
                    defaultBuilder.authorityHost(authorityHost);
                }
                return defaultBuilder.build(); // CodeQL [SM05141] Use DefaultAzureCredential explicitly for local development and is decided by the user
            case "managedidentity":
                return new ManagedIdentityCredentialBuilder().clientId(getClientId()).build();
            case "workloadidentity":
                WorkloadIdentityCredentialBuilder builder = new WorkloadIdentityCredentialBuilder();
                if (hasAuthorityHost) {
                    builder.authorityHost(authorityHost);
                }
                if (getClientId() != null && !getClientId().isEmpty()) {
                    builder.clientId(getClientId());
                }
                
                if (getTenantId() != null && !getTenantId().isEmpty()) {
                    builder.tenantId(getTenantId());
                }
                                
                if (getTokenFilePath() != null && !getTokenFilePath().isEmpty()) {
                    builder.tokenFilePath(getTokenFilePath());
                }

                if (getAdditionallyAllowedTenants() != null) {
                    for (String tenant : getAdditionallyAllowedTenants()) {
                        builder.additionallyAllowedTenants(tenant);
                    }
                }

                return builder.build();
            case "environment":
                EnvironmentCredentialBuilder environmentBuilder = new EnvironmentCredentialBuilder();
                if (hasAuthorityHost) {
                    environmentBuilder.authorityHost(authorityHost);
                }
                return environmentBuilder.build();
            case "azurecli":
                return new AzureCliCredentialBuilder().build();
            case "azurepowershell":
                return new AzurePowerShellCredentialBuilder().build();
            case "visualstudiocode":
                return new VisualStudioCodeCredentialBuilder().build();
            case "intellij":
                return new IntelliJCredentialBuilder().build();
            case "interactivebrowser":
                InteractiveBrowserCredentialBuilder browserBuilder = new InteractiveBrowserCredentialBuilder();
                if (hasAuthorityHost) {
                    browserBuilder.authorityHost(authorityHost);
                }
                return browserBuilder.build();
            case "none":
                return null;
            default:
                throw new IllegalArgumentException(
                    String.format("The connection string contains an unsupported authentication type '%s'.", authType));
        }
    }
} 