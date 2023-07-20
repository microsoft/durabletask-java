// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azurefunctions;

import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;

import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * The binding value type for the {@literal @}DurableClientInput parameter.
 */
public class DurableClientContext {
    // These fields are populated via GSON deserialization by the Functions Java worker.
    private String rpcBaseUrl;
    private String taskHubName;
    private String requiredQueryStringParameters;
    private DurableTaskClient client;

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

        this.client = new DurableTaskGrpcClientBuilder().port(rpcURL.getPort()).build();
        return this.client;
    }

    /**
     * Creates an HTTP response which either contains a payload of management URLs for a non-completed instance
     * or contains the payload containing the output of the completed orchestration.
     * <p>
     * If the orchestration instance completes within the specified timeout, then the HTTP response payload will
     * contains the output of the orchestration instance formatted as JSON. However, if the orchestration does not
     * complete within the specified timeout, then the HTTP response will be identical to that of the
     * {@link #createCheckStatusResponse(HttpRequestMessage, String)} API.
     * </p>
     * @param request the HTTP request that triggered the current function
     * @param instanceId the unique ID of the instance to check
     * @param timeout total allowed timeout for output from the durable function
     * @return an HTTP response which may include a 202 and location header or a 200 with the durable function output in the response body
     */
    public HttpResponseMessage waitForCompletionOrCreateCheckStatusResponse(
            HttpRequestMessage<?> request,
            String instanceId,
            Duration timeout) {
        if (this.client == null) {
            this.client = getClient();
        }
        OrchestrationMetadata orchestration;
        try {
            orchestration = this.client.waitForInstanceCompletion(instanceId, timeout, true);
            return request.createResponseBuilder(HttpStatus.ACCEPTED)
                    .header("Content-Type", "application/json")
                    .body(orchestration.getSerializedOutput())
                    .build();
        } catch (TimeoutException e) {
            return createCheckStatusResponse(request, instanceId);
        }
    }

    /**
     * Creates an HTTP response that is useful for checking the status of the specified instance.
     * <p>
     * The payload of the returned
     * @see <a href="https://learn.microsoft.com/java/api/com.microsoft.azure.functions.httpresponsemessage">HttpResponseMessage</a>
     * contains HTTP API URLs that can be used to query the status of the orchestration, raise events to the orchestration, or
     * terminate the orchestration.
     * @param request the HTTP request that triggered the current orchestration instance
     * @param instanceId the ID of the orchestration instance to check
     * @return an HTTP 202 response with a Location header and a payload containing instance control URLs
     */
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
}
