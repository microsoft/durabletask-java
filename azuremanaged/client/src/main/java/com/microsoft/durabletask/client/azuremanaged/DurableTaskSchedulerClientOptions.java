// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.client.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.microsoft.durabletask.shared.azuremanaged.DurableTaskSchedulerConnectionString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import java.util.Objects;

/**
 * Options for configuring the Durable Task Scheduler.
 */
public class DurableTaskSchedulerClientOptions {
    @NotBlank(message = "Endpoint address is required")
    private String endpointAddress = "";

    @NotBlank(message = "Task hub name is required")
    private String taskHubName = "";

    private TokenCredential credential;
    private String resourceId = "https://durabletask.io";
    private boolean allowInsecureCredentials = false;
    private Duration tokenRefreshMargin = Duration.ofMinutes(5);

    /**
     * Creates a new instance of DurableTaskSchedulerClientOptions.
     */
    public DurableTaskSchedulerClientOptions() {
    }

    /**
     * Creates a new instance of DurableTaskSchedulerClientOptions from a connection string.
     * 
     * @param connectionString The connection string to parse.
     * @return A new DurableTaskSchedulerClientOptions object.
     */
    public static DurableTaskSchedulerClientOptions fromConnectionString(String connectionString) {
        DurableTaskSchedulerConnectionString parsedConnectionString = DurableTaskSchedulerConnectionString.parse(connectionString);
        return fromConnectionString(parsedConnectionString);
    }

    /**
     * Creates a new instance of DurableTaskSchedulerClientOptions from a parsed connection string.
     * 
     * @param connectionString The parsed connection string.
     * @return A new DurableTaskSchedulerClientOptions object.
     */
    static DurableTaskSchedulerClientOptions fromConnectionString(DurableTaskSchedulerConnectionString connectionString) {
        TokenCredential credential = getCredentialFromConnectionString(connectionString);
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions();
        options.setEndpointAddress(connectionString.getEndpoint());
        options.setTaskHubName(connectionString.getTaskHubName());
        options.setCredential(credential);
        options.setAllowInsecureCredentials(credential == null);
        return options;
    }

    /**
     * Gets the endpoint address.
     * 
     * @return The endpoint address.
     */
    public String getEndpointAddress() {
        return endpointAddress;
    }

    /**
     * Sets the endpoint address.
     * 
     * @param endpointAddress The endpoint address.
     * @return This options object.
     */
    public DurableTaskSchedulerClientOptions setEndpointAddress(String endpointAddress) {
        this.endpointAddress = endpointAddress;
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
    public DurableTaskSchedulerClientOptions setTaskHubName(String taskHubName) {
        this.taskHubName = taskHubName;
        return this;
    }

    /**
     * Gets the credential used for authentication.
     * 
     * @return The credential.
     */
    public TokenCredential getCredential() {
        return credential;
    }

    /**
     * Sets the credential used for authentication.
     * 
     * @param credential The credential.
     * @return This options object.
     */
    public DurableTaskSchedulerClientOptions setCredential(TokenCredential credential) {
        this.credential = credential;
        return this;
    }

    /**
     * Gets the resource ID.
     * 
     * @return The resource ID.
     */
    public String getResourceId() {
        return resourceId;
    }

    /**
     * Sets the resource ID.
     * 
     * @param resourceId The resource ID.
     * @return This options object.
     */
    public DurableTaskSchedulerClientOptions setResourceId(String resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    /**
     * Gets whether insecure credentials are allowed.
     * 
     * @return True if insecure credentials are allowed.
     */
    public boolean isAllowInsecureCredentials() {
        return allowInsecureCredentials;
    }

    /**
     * Sets whether insecure credentials are allowed.
     * 
     * @param allowInsecureCredentials True to allow insecure credentials.
     * @return This options object.
     */
    public DurableTaskSchedulerClientOptions setAllowInsecureCredentials(boolean allowInsecureCredentials) {
        this.allowInsecureCredentials = allowInsecureCredentials;
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
    public DurableTaskSchedulerClientOptions setTokenRefreshMargin(Duration tokenRefreshMargin) {
        this.tokenRefreshMargin = tokenRefreshMargin;
        return this;
    }

    /**
     * Creates a gRPC channel for communicating with the Durable Task Scheduler service.
     * 
     * @return A configured ManagedChannel instance.
     */
    public ManagedChannel createChannel() {
        Objects.requireNonNull(endpointAddress, "endpointAddress must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");

        String endpoint = !endpointAddress.contains("://") ? "https://" + endpointAddress : endpointAddress;
        
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("taskhub", Metadata.ASCII_STRING_MARSHALLER), taskHubName);

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(endpoint);
        
        if (endpoint.startsWith("https://")) {
            channelBuilder.useTransportSecurity();
        } else {
            channelBuilder.usePlaintext();
        }

        if (credential != null) {
            // TODO: Implement token credential handling for gRPC
            // This would require implementing a custom CallCredentials class
        }

        return channelBuilder.build();
    }

    /**
     * Gets the credential from a connection string.
     * 
     * @param connectionString The connection string.
     * @return The credential.
     */
    private static TokenCredential getCredentialFromConnectionString(DurableTaskSchedulerConnectionString connectionString) {
        String authType = connectionString.getAuthentication().toLowerCase();
        
        // TODO: Implement credential creation based on auth type
        // This would require implementing the various credential types
        return null;
    }

    /**
     * Validates that the options are properly configured.
     * 
     * @throws IllegalArgumentException If the options are not properly configured.
     */
    public void validate() {
        Objects.requireNonNull(endpointAddress, "endpointAddress must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");
    }
}