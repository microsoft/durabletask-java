// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azurefunctions;

import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;

import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * The binding value type for the {@literal @}DurableClientInput parameter.
 */
public class DurableClientContext {
    // These fields are populated via GSON deserialization by the Functions Java worker.
    private String rpcBaseUrl;
    private String taskHubName;
    private String requiredQueryStringParameters;

    /**
     * Gets the name of the client binding's task hub.
     *
     * @return the name of the client binding's task hub.
     */
    public String getTaskHubName() {
        return this.taskHubName;
    }

    /**
     * Gets the durable task client associated with the current function invocation.
     *
     * @return the Durable Task client object associated with the current function invocation.
     */
    public DurableTaskClient getClient() {
        if (this.rpcBaseUrl == null || this.rpcBaseUrl.length() == 0) {
            throw new IllegalStateException("The client context wasn't populated with an RPC base URL!");
        }

        URL rpcURL;
        try {
            rpcURL = new URL(this.rpcBaseUrl);
        } catch (MalformedURLException ex) {
            throw new IllegalStateException("The client context RPC base URL was invalid!", ex);
        }

        return new DurableTaskGrpcClientBuilder().port(rpcURL.getPort()).build();
    }

    public HttpResponseMessage createCheckStatusResponse(HttpRequestMessage<?> request, String instanceId) {
        // TODO: To better support scenarios involving proxies or application gateways, this
        //       code should take the X-Forwarded-Host, X-Forwarded-Proto, and Forwarded HTTP
        //       request headers into consideration and generate the base URL accordingly.
        //       More info: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Forwarded.
        //       One potential workaround is to set ASPNETCORE_FORWARDEDHEADERS_ENABLED to true.

        // Construct the response as an HTTP 202 with a JSON object payload
        return request.createResponseBuilder(HttpStatus.ACCEPTED)
                .header("Location", this.getInstanceStatusURL(request, instanceId) + "?" + this.requiredQueryStringParameters)
                .header("Content-Type", "application/json")
                .body(this.getClientResponseLinks(request, instanceId))
                .build();
    }

    /**
     * Creates a {@link HttpManagementPayload} that is useful for checking the status of the specified instance.
     *
     * @param request    The HTTP request that triggered the current orchestration instance.
     * @param instanceId The ID of the orchestration instance to check.
     * @return The {@link HttpManagementPayload} with URLs that can be used to query the status of the orchestration,
     *         raise events to the orchestration, or terminate the orchestration.
     */
    public HttpManagementPayload createHttpManagementPayload(HttpRequestMessage<?> request, String instanceId) {
        return this.getClientResponseLinks(request, instanceId);
    }

    private HttpManagementPayload getClientResponseLinks(HttpRequestMessage<?> request, String instanceId) {
        String instanceStatusURL = this.getInstanceStatusURL(request, instanceId);
        return new HttpManagementPayload(instanceId, instanceStatusURL, this.requiredQueryStringParameters);
    }

    private String getInstanceStatusURL(HttpRequestMessage<?> request, String instanceId) {
        String baseUrl = request.getUri().getScheme() + "://" + request.getUri().getAuthority();
        String encodedInstanceId;

        try {
            encodedInstanceId = URLEncoder.encode(instanceId, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalArgumentException("Failed to encode the instance ID: " + instanceId, ex);
        }

        return baseUrl + "/runtime/webhooks/durabletask/instances/" + encodedInstanceId;
    }

    public static class HttpManagementPayload {
        private final String id;
        private final String purgeHistoryDeleteUri;
        private final String sendEventPostUri;
        private final String statusQueryGetUri;
        private final String terminatePostUri;

        public HttpManagementPayload(
                String instanceId,
                String instanceStatusURL,
                String requiredQueryStringParameters) {
            this.id = instanceId;
            this.purgeHistoryDeleteUri = instanceStatusURL + "?" + requiredQueryStringParameters;
            this.sendEventPostUri = instanceStatusURL + "/raiseEvent/{eventName}?" + requiredQueryStringParameters;
            this.statusQueryGetUri = instanceStatusURL + "?" + requiredQueryStringParameters;
            this.terminatePostUri = instanceStatusURL + "/terminate?reason={text}&" + requiredQueryStringParameters;
        }

        /**
         * Gets the ID of the orchestration instance.
         *
         * @return The ID of the orchestration instance.
         */
        public String getId() {
            return this.id;
        }

        /**
         * Gets the HTTP GET status query endpoint URL.
         *
         * @return The HTTP URL for fetching the instance status.
         */
        public String getStatusQueryGetUri() {
            return this.statusQueryGetUri;
        }

        /**
         * Gets the HTTP POST external event sending endpoint URL.
         *
         * @return The HTTP URL for posting external event notifications.
         */
        public String getSendEventPostUri() {
            return this.sendEventPostUri;
        }

        /**
         * Gets the HTTP POST instance termination endpoint.
         *
         * @return The HTTP URL for posting instance termination commands.
         */
        public String getTerminatePostUri() {
            return this.terminatePostUri;
        }

        /**
         * Gets the HTTP DELETE purge instance history by instance ID endpoint.
         *
         * @return The HTTP URL for purging instance history by instance ID.
         */
        public String getPurgeHistoryDeleteUri() {
            return this.purgeHistoryDeleteUri;
        }
    }
}
