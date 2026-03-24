// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableHttpRequest;
import com.microsoft.durabletask.DurableHttpResponse;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.HttpRetryOptions;
import com.microsoft.durabletask.ManagedIdentityTokenSource;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Sample Azure Functions demonstrating different {@code callHttp} usage patterns
 * with Durable Functions orchestrations.
 */
public class CallHttpFunctions {

    // =====================================================================
    // 1. Simple GET using the convenience method
    // =====================================================================

    @FunctionName("StartSimpleGetOrchestration")
    public HttpResponseMessage startSimpleGet(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("SimpleGetOrchestrator");
        context.getLogger().info("Started SimpleGetOrchestrator with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("SimpleGetOrchestrator")
    public String simpleGetOrchestrator(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        // Simple GET request using the convenience overload (method + URI only)
        DurableHttpResponse response = ctx.callHttp("GET",
                URI.create("https://httpbin.org/get")).await();

        return "Status: " + response.getStatusCode() + ", Body: " + response.getContent();
    }

    // =====================================================================
    // 2. POST with custom headers and JSON body
    // =====================================================================

    @FunctionName("StartPostWithHeadersOrchestration")
    public HttpResponseMessage startPostWithHeaders(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("PostWithHeadersOrchestrator");
        context.getLogger().info("Started PostWithHeadersOrchestrator with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("PostWithHeadersOrchestrator")
    public String postWithHeadersOrchestrator(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        // POST request with custom headers and a JSON body
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "my-value");

        String jsonBody = "{\"name\": \"Durable Functions\", \"language\": \"Java\"}";

        DurableHttpResponse response = ctx.callHttp("POST",
                URI.create("https://httpbin.org/post"),
                headers,
                jsonBody).await();

        return "Status: " + response.getStatusCode() + ", Body: " + response.getContent();
    }

    // =====================================================================
    // 3. Managed Identity token source for calling Azure resources
    // =====================================================================

    @FunctionName("StartManagedIdentityOrchestration")
    public HttpResponseMessage startManagedIdentity(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("ManagedIdentityOrchestrator");
        context.getLogger().info("Started ManagedIdentityOrchestrator with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("ManagedIdentityOrchestrator")
    public String managedIdentityOrchestrator(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        // Use ManagedIdentityTokenSource to automatically attach a bearer token
        // when calling Azure Resource Manager APIs. The resource URI is auto-normalized
        // to include "/.default" for well-known Azure resource bases.
        ManagedIdentityTokenSource tokenSource =
                new ManagedIdentityTokenSource("https://management.core.windows.net");

        DurableHttpRequest request = new DurableHttpRequest(
                "GET",
                URI.create("https://management.azure.com/subscriptions?api-version=2020-01-01"),
                null,  // headers
                null,  // content
                tokenSource);

        DurableHttpResponse response = ctx.callHttp(request).await();

        return "Status: " + response.getStatusCode() + ", Body: " + response.getContent();
    }

    // =====================================================================
    // 4. HTTP retry options with backoff and status code filtering
    // =====================================================================

    @FunctionName("StartRetryableHttpOrchestration")
    public HttpResponseMessage startRetryableHttp(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("RetryableHttpOrchestrator");
        context.getLogger().info("Started RetryableHttpOrchestrator with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("RetryableHttpOrchestrator")
    public String retryableHttpOrchestrator(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        // Configure HTTP-level retry with exponential backoff and specific status codes
        HttpRetryOptions retryOptions = new HttpRetryOptions(Duration.ofSeconds(5), 3);
        retryOptions.setBackoffCoefficient(2.0);
        retryOptions.setMaxRetryInterval(Duration.ofMinutes(1));
        retryOptions.setRetryTimeout(Duration.ofMinutes(5));
        retryOptions.setStatusCodesToRetry(Arrays.asList(500, 502, 503, 504));

        DurableHttpRequest request = new DurableHttpRequest(
                "GET",
                URI.create("https://httpbin.org/status/200"),
                null,   // headers
                null,   // content
                null,   // tokenSource
                true,   // asynchronousPatternEnabled
                Duration.ofMinutes(10),  // timeout
                retryOptions);

        DurableHttpResponse response = ctx.callHttp(request).await();

        return "Status: " + response.getStatusCode() + ", Body: " + response.getContent();
    }
}
