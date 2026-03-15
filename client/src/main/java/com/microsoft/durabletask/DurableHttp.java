// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Provides static helper methods for making durable HTTP calls from orchestrator functions.
 * <p>
 * The {@code callHttp} methods schedule a built-in HTTP activity that is executed by the Durable Task host.
 * The HTTP request and response are serialized and persisted in the orchestration history, making them safe
 * for replay.
 * <p>
 * Example usage:
 * <pre>{@code
 * DurableHttpRequest request = new DurableHttpRequest("GET", new URI("https://example.com/api/data"));
 * DurableHttpResponse response = DurableHttp.callHttp(ctx, request).await();
 * return response.getContent();
 * }</pre>
 * <p>
 * <b>Asynchronous HTTP 202 Polling:</b> When {@link DurableHttpRequest#isAsynchronousPatternEnabled()} is
 * {@code true} (the default) and the target endpoint returns an HTTP 202 response with a {@code Location}
 * header, the framework will automatically poll the location URL until a non-202 response is received.
 * <p>
 * <b>Managed Identity Authentication:</b> For authenticated calls using Azure Managed Identity, configure
 * a {@link ManagedIdentityTokenSource} on the request:
 * <pre>{@code
 * TokenSource tokenSource = new ManagedIdentityTokenSource("https://management.core.windows.net/.default");
 * DurableHttpRequest request = new DurableHttpRequest(
 *     "POST",
 *     new URI("https://management.azure.com/subscriptions/.../restart?api-version=2019-03-01"),
 *     null, null, tokenSource);
 * DurableHttpResponse response = DurableHttp.callHttp(ctx, request).await();
 * }</pre>
 *
 * @see DurableHttpRequest
 * @see DurableHttpResponse
 * @see TaskOrchestrationContext
 */
public final class DurableHttp {

    /**
     * The well-known built-in activity name recognized by the Durable Task host for HTTP calls.
     */
    public static final String BUILT_IN_HTTP_ACTIVITY_NAME = "BuiltIn::HttpActivity";

    private DurableHttp() {
        // Static utility class — not instantiable
    }

    /**
     * Makes a durable HTTP request using the specified {@link DurableHttpRequest} and returns a {@link Task}
     * that completes when the HTTP call completes. The returned {@code Task}'s value will be a
     * {@link DurableHttpResponse} containing the status code, headers, and content of the HTTP response.
     * <p>
     * If the HTTP call results in a failure (e.g., a connection error), the returned {@code Task} will complete
     * exceptionally with a {@link TaskFailedException}. Note that HTTP error status
     * codes (4xx, 5xx) are <em>not</em> treated as failures — the response will be returned normally with the
     * corresponding status code.
     *
     * @param ctx the orchestration context
     * @param request the {@link DurableHttpRequest} describing the HTTP request to make
     * @return a new {@link Task} that completes when the HTTP call completes
     */
    public static Task<DurableHttpResponse> callHttp(TaskOrchestrationContext ctx, DurableHttpRequest request) {
        return callHttp(ctx, request, null);
    }

    /**
     * Makes a durable HTTP request using the specified {@link DurableHttpRequest} with additional
     * options (e.g., retry policies) and returns a {@link Task} that completes when the HTTP call completes.
     *
     * @param ctx the orchestration context
     * @param request the {@link DurableHttpRequest} describing the HTTP request to make
     * @param options additional options that control the execution and processing of the HTTP call
     *                (e.g., retry policies), or {@code null} for default behavior
     * @return a new {@link Task} that completes when the HTTP call completes
     */
    public static Task<DurableHttpResponse> callHttp(TaskOrchestrationContext ctx, DurableHttpRequest request,
                                                      @Nullable TaskOptions options) {
        if (ctx == null) {
            throw new IllegalArgumentException("ctx must not be null");
        }
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        // The Durable Functions host expects the input as a JSON array of DurableHttpRequest objects.
        List<DurableHttpRequest> wrappedInput = Collections.singletonList(request);
        if (options != null) {
            return ctx.callActivity(BUILT_IN_HTTP_ACTIVITY_NAME, wrappedInput, options, DurableHttpResponse.class);
        }
        return ctx.callActivity(BUILT_IN_HTTP_ACTIVITY_NAME, wrappedInput, DurableHttpResponse.class);
    }

    /**
     * Makes a simple durable HTTP request to the specified URI.
     * <p>
     * This is a convenience method equivalent to:
     * <pre>{@code
     * DurableHttp.callHttp(ctx, new DurableHttpRequest(method, uri))
     * }</pre>
     *
     * @param ctx the orchestration context
     * @param method the HTTP method (e.g., "GET", "POST", "PUT", "DELETE")
     * @param uri the target URI for the HTTP request
     * @return a new {@link Task} that completes when the HTTP call completes
     */
    public static Task<DurableHttpResponse> callHttp(TaskOrchestrationContext ctx, String method, URI uri) {
        return callHttp(ctx, new DurableHttpRequest(method, uri));
    }

    /**
     * Makes a durable HTTP request with headers and content to the specified URI.
     *
     * @param ctx the orchestration context
     * @param method the HTTP method (e.g., "GET", "POST", "PUT", "DELETE")
     * @param uri the target URI for the HTTP request
     * @param headers the HTTP headers to include in the request, or {@code null} for no headers
     * @param content the body content of the HTTP request, or {@code null} for no body
     * @return a new {@link Task} that completes when the HTTP call completes
     */
    public static Task<DurableHttpResponse> callHttp(TaskOrchestrationContext ctx, String method, URI uri,
                                                      @Nullable Map<String, String> headers,
                                                      @Nullable String content) {
        return callHttp(ctx, new DurableHttpRequest(method, uri, headers, content));
    }
}
