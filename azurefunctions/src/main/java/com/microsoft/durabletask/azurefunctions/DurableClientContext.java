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
        String baseUrl = request.getUri().getScheme() + "://" + request.getUri().getAuthority();
        String encodedInstanceId;
        try {
            encodedInstanceId = URLEncoder.encode(instanceId, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalArgumentException("Failed to encode the instance ID: " + instanceId, ex);
        }

        String instanceStatusURL = baseUrl + "/runtime/webhooks/durabletask/instances/" + encodedInstanceId;

        // Construct the response as an HTTP 201 with a JSON object payload
        return request.createResponseBuilder(HttpStatus.CREATED)
                .header("Location", instanceStatusURL + "?" + this.requiredQueryStringParameters)
                .header("Content-Type", "application/json")
                .body(new HttpCreateCheckStatusResponse(
                        instanceId,
                        instanceStatusURL,
                        this.requiredQueryStringParameters))
                .build();
            }

    private static class HttpCreateCheckStatusResponse {
        // These fields are serialized to JSON
        public final String id;
        public final String purgeHistoryDeleteUri;
        public final String sendEventPostUri;
        public final String statusQueryGetUri;
        public final String terminatePostUri;

        public HttpCreateCheckStatusResponse(
                String instanceId,
                String instanceStatusURL,
                String requiredQueryStringParameters) {
            this.id = instanceId;
            this.purgeHistoryDeleteUri = instanceStatusURL + "?" + requiredQueryStringParameters;
            this.sendEventPostUri = instanceStatusURL + "/raiseEvent/{eventName}?" + requiredQueryStringParameters;
            this.statusQueryGetUri = instanceStatusURL + "?" + requiredQueryStringParameters;
            this.terminatePostUri = instanceStatusURL + "/terminate?reason={text}&" + requiredQueryStringParameters;
        }
    }
}
