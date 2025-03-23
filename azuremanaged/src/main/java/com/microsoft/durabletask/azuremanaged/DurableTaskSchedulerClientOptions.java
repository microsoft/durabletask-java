// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import io.grpc.*;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.Objects;
import java.net.URL;

/**
 * Options for configuring the Durable Task Scheduler.
 */
public class DurableTaskSchedulerClientOptions {
    private String endpointAddress = "";
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
        DurableTaskSchedulerConnectionString parsedConnectionString = new DurableTaskSchedulerConnectionString(connectionString);
        return fromConnectionString(parsedConnectionString);
    }

    /**
     * Creates a new instance of DurableTaskSchedulerClientOptions from a parsed connection string.
     * 
     * @param connectionString The parsed connection string.
     * @return A new DurableTaskSchedulerClientOptions object.
     */
    static DurableTaskSchedulerClientOptions fromConnectionString(DurableTaskSchedulerConnectionString connectionString) {
        // TODO: Parse different credential types from connection string
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions();
        options.setEndpointAddress(connectionString.getEndpoint());
        options.setTaskHubName(connectionString.getTaskHubName());
        options.setCredential(connectionString.getCredential());
        options.setAllowInsecureCredentials(options.getCredential() == null);
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
     * Creates a gRPC channel using the configured options.
     * 
     * @return A configured gRPC channel for communication with the Durable Task service.
     */
    public Channel createGrpcChannel() {
        // Create token cache only if credential is not null
        AccessTokenCache tokenCache = null;
        if (credential != null) {
            TokenRequestContext context = new TokenRequestContext();
            context.addScopes(new String[] { this.resourceId + "/.default" });
            tokenCache = new AccessTokenCache(this.credential, context, this.tokenRefreshMargin);
        }

        // Parse and normalize the endpoint URL
        String endpoint = endpointAddress;
        // Add https:// prefix if no protocol is specified
        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            endpoint = "https://" + endpoint;
        }
        
        URL url;
        try {
            url = new URL(endpoint);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid endpoint URL: " + endpoint);
        }

        String authority = url.getHost();
        if (url.getPort() != -1) {
            authority += ":" + url.getPort();
        }
        
        // Create metadata interceptor to add task hub name and auth token
        AccessTokenCache finalTokenCache = tokenCache;
        ClientInterceptor metadataInterceptor = new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                        next.newCall(method, callOptions)) {
                    @Override
                    public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
                        headers.put(
                            Metadata.Key.of("taskhub", Metadata.ASCII_STRING_MARSHALLER),
                            taskHubName
                        );
                        
                        // Add authorization token if credentials are configured
                        if (finalTokenCache != null) {
                            String token = finalTokenCache.getToken().getToken();
                            headers.put(
                                Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER),
                                "Bearer " + token
                            );
                        }
                        
                        super.start(responseListener, headers);
                    }
                };
            }
        };
        
        ChannelCredentials credentials;
        if (!this.allowInsecureCredentials) {
            credentials = io.grpc.TlsChannelCredentials.create();
        } else {
            credentials = InsecureChannelCredentials.create();
        }

        // Create channel with credentials
        return Grpc.newChannelBuilder(authority, credentials)
                .intercept(metadataInterceptor)
                .build();
    }
}