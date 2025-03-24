// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.TlsChannelCredentials;
import io.grpc.ClientInterceptor;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.CallOptions;
import io.grpc.ForwardingClientCall;

import java.time.Duration;
import java.util.Objects;
import java.net.URL;
import java.net.MalformedURLException;

/**
 * Options for configuring the Durable Task Scheduler worker.
 */
public class DurableTaskSchedulerWorkerOptions {
    private String endpointAddress = "";
    private String taskHubName = "";

    private TokenCredential credential;
    private String resourceId = "https://durabletask.io";
    private boolean allowInsecureCredentials = false;
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
        DurableTaskSchedulerConnectionString parsedConnectionString = new DurableTaskSchedulerConnectionString(connectionString);
        return fromConnectionString(parsedConnectionString);
    }

    /**
     * Creates a new instance of DurableTaskSchedulerWorkerOptions from a parsed connection string.
     * 
     * @param connectionString The parsed connection string.
     * @return A new DurableTaskSchedulerWorkerOptions object.
     */
    static DurableTaskSchedulerWorkerOptions fromConnectionString(DurableTaskSchedulerConnectionString connectionString) {
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions();
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
    public DurableTaskSchedulerWorkerOptions setEndpointAddress(String endpointAddress) {
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
    public DurableTaskSchedulerWorkerOptions setTaskHubName(String taskHubName) {
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
    public DurableTaskSchedulerWorkerOptions setCredential(TokenCredential credential) {
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
    public DurableTaskSchedulerWorkerOptions setResourceId(String resourceId) {
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
    public DurableTaskSchedulerWorkerOptions setAllowInsecureCredentials(boolean allowInsecureCredentials) {
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
    public DurableTaskSchedulerWorkerOptions setTokenRefreshMargin(Duration tokenRefreshMargin) {
        this.tokenRefreshMargin = tokenRefreshMargin;
        return this;
    }

    /**
     * Creates a gRPC channel using the configured options.
     * 
     * @return A configured gRPC channel.
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

        // Configure channel credentials based on endpoint protocol
        ChannelCredentials credentials;
        if (endpoint.toLowerCase().startsWith("https://")) {
            credentials = TlsChannelCredentials.create();
        } else {
            credentials = InsecureChannelCredentials.create();
        }

        // Create channel with credentials
        return Grpc.newChannelBuilder(authority, credentials)
                .intercept(metadataInterceptor)
                .build();
    }
} 