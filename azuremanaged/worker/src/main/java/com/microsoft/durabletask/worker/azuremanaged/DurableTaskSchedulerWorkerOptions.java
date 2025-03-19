// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.worker.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.microsoft.durabletask.shared.azuremanaged.DurableTaskSchedulerConnectionString;
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

import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import java.util.Objects;
import java.net.URL;
import java.net.MalformedURLException;
import javax.annotation.Nullable;

/**
 * Options for configuring the Durable Task Scheduler worker.
 */
public class DurableTaskSchedulerWorkerOptions {
    @NotBlank(message = "Endpoint address is required")
    private String endpointAddress = "";

    @NotBlank(message = "Task hub name is required")
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
     * @param credential The token credential for authentication, or null to use connection string credentials.
     * @return A new DurableTaskSchedulerWorkerOptions object.
     */
    public static DurableTaskSchedulerWorkerOptions fromConnectionString(String connectionString, @Nullable TokenCredential credential) {
        DurableTaskSchedulerConnectionString parsedConnectionString = DurableTaskSchedulerConnectionString.parse(connectionString);
        return fromConnectionString(parsedConnectionString, credential);
    }

    /**
     * Creates a new instance of DurableTaskSchedulerWorkerOptions from a parsed connection string.
     * 
     * @param connectionString The parsed connection string.
     * @param credential The token credential for authentication, or null to use connection string credentials.
     * @return A new DurableTaskSchedulerWorkerOptions object.
     */
    static DurableTaskSchedulerWorkerOptions fromConnectionString(DurableTaskSchedulerConnectionString connectionString, @Nullable TokenCredential credential) {
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions();
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
     * Validates that the options are properly configured.
     * 
     * @throws IllegalArgumentException If the options are not properly configured.
     */
    public void validate() {
        Objects.requireNonNull(endpointAddress, "endpointAddress must not be null");
        Objects.requireNonNull(taskHubName, "taskHubName must not be null");
    }

    /**
     * Creates a gRPC channel using the configured options.
     * 
     * @return A configured gRPC channel.
     * @throws MalformedURLException If the endpoint address is invalid.
     */
    public Channel createGrpcChannel() throws MalformedURLException {
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
        
        URL url = new URL(endpoint);
        String authority = url.getHost();
        if (url.getPort() != -1) {
            authority += ":" + url.getPort();
        }
        
        // Create metadata interceptor to add task hub name and auth token
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
                        if (tokenCache != null) {
                            String token = tokenCache.getToken().getToken();
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