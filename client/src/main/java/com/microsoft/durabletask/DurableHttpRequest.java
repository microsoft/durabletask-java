// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an HTTP request that can be made by an orchestrator function using
 * {@link DurableHttp#callHttp}.
 * <p>
 * This class is used to configure HTTP requests that are executed durably by the Durable Functions host.
 * The request and response are serialized and persisted in the orchestration history, making them safe
 * for replay.
 * <p>
 * Example usage:
 * <pre>{@code
 * DurableHttpRequest request = new DurableHttpRequest("GET", new URI("https://example.com/api/data"));
 * DurableHttpResponse response = DurableHttp.callHttp(ctx, request).await();
 * }</pre>
 * <p>
 * <b>With timeout and retry:</b>
 * <pre>{@code
 * HttpRetryOptions retryOptions = new HttpRetryOptions(Duration.ofSeconds(5), 3);
 * DurableHttpRequest request = new DurableHttpRequest(
 *     "GET", new URI("https://example.com/api/data"),
 *     null, null, null, true,
 *     Duration.ofMinutes(10), retryOptions);
 * }</pre>
 *
 * @see DurableHttpResponse
 * @see DurableHttp#callHttp
 * @see HttpRetryOptions
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DurableHttpRequest {

    private final String method;
    private final URI uri;
    private final Map<String, String> headers;
    private final String content;
    private final TokenSource tokenSource;
    private final boolean asynchronousPatternEnabled;
    private final String timeout; // Stored as .NET TimeSpan format string for wire compatibility
    private final HttpRetryOptions httpRetryOptions;

    /**
     * Creates a new {@code DurableHttpRequest} with the specified HTTP method and URI.
     *
     * @param method the HTTP method (e.g., "GET", "POST", "PUT", "DELETE")
     * @param uri the target URI for the HTTP request
     */
    public DurableHttpRequest(String method, URI uri) {
        this(method, uri, null, null, null, true, (String) null, null);
    }

    /**
     * Creates a new {@code DurableHttpRequest} with the specified HTTP method, URI, and headers.
     *
     * @param method the HTTP method (e.g., "GET", "POST", "PUT", "DELETE")
     * @param uri the target URI for the HTTP request
     * @param headers the HTTP headers to include in the request, or {@code null} for no headers
     */
    public DurableHttpRequest(String method, URI uri, @Nullable Map<String, String> headers) {
        this(method, uri, headers, null, null, true, (String) null, null);
    }

    /**
     * Creates a new {@code DurableHttpRequest} with the specified HTTP method, URI, headers, and content.
     *
     * @param method the HTTP method (e.g., "GET", "POST", "PUT", "DELETE")
     * @param uri the target URI for the HTTP request
     * @param headers the HTTP headers to include in the request, or {@code null} for no headers
     * @param content the body content of the HTTP request, or {@code null} for no body
     */
    public DurableHttpRequest(String method, URI uri, @Nullable Map<String, String> headers, @Nullable String content) {
        this(method, uri, headers, content, null, true, (String) null, null);
    }

    /**
     * Creates a new {@code DurableHttpRequest} with the specified HTTP method, URI, headers, content,
     * and token source.
     *
     * @param method the HTTP method (e.g., "GET", "POST", "PUT", "DELETE")
     * @param uri the target URI for the HTTP request
     * @param headers the HTTP headers to include in the request, or {@code null} for no headers
     * @param content the body content of the HTTP request, or {@code null} for no body
     * @param tokenSource the token source for authentication, or {@code null} for no authentication
     */
    public DurableHttpRequest(String method, URI uri, @Nullable Map<String, String> headers,
                              @Nullable String content, @Nullable TokenSource tokenSource) {
        this(method, uri, headers, content, tokenSource, true, (String) null, null);
    }

    /**
     * Creates a new {@code DurableHttpRequest} with the specified HTTP method, URI, headers, content,
     * token source, and asynchronous pattern setting.
     *
     * @param method the HTTP method (e.g., "GET", "POST", "PUT", "DELETE")
     * @param uri the target URI for the HTTP request
     * @param headers the HTTP headers to include in the request, or {@code null} for no headers
     * @param content the body content of the HTTP request, or {@code null} for no body
     * @param tokenSource the token source for authentication, or {@code null} for no authentication
     * @param asynchronousPatternEnabled {@code true} to enable automatic HTTP 202 polling, {@code false} to disable
     */
    public DurableHttpRequest(String method, URI uri, @Nullable Map<String, String> headers,
                              @Nullable String content, @Nullable TokenSource tokenSource,
                              boolean asynchronousPatternEnabled) {
        this(method, uri, headers, content, tokenSource, asynchronousPatternEnabled, (String) null, null);
    }

    /**
     * Creates a new {@code DurableHttpRequest} with all configurable options including timeout and retry.
     *
     * @param method the HTTP method (e.g., "GET", "POST", "PUT", "DELETE")
     * @param uri the target URI for the HTTP request
     * @param headers the HTTP headers to include in the request, or {@code null} for no headers
     * @param content the body content of the HTTP request, or {@code null} for no body
     * @param tokenSource the token source for authentication, or {@code null} for no authentication
     * @param asynchronousPatternEnabled {@code true} to enable automatic HTTP 202 polling, {@code false} to disable
     * @param timeout the total timeout for the HTTP request and any asynchronous polling, or {@code null} for no timeout
     * @param httpRetryOptions the retry options for the HTTP request, or {@code null} for no retries
     */
    public DurableHttpRequest(String method, URI uri, @Nullable Map<String, String> headers,
                              @Nullable String content, @Nullable TokenSource tokenSource,
                              boolean asynchronousPatternEnabled, @Nullable Duration timeout,
                              @Nullable HttpRetryOptions httpRetryOptions) {
        this(method, uri, headers, content, tokenSource, asynchronousPatternEnabled,
                timeout != null ? TimeSpanHelper.format(timeout) : null, httpRetryOptions);
    }

    /**
     * Primary internal constructor used by Jackson for deserialization.
     * Takes timeout as a raw .NET TimeSpan format string for wire compatibility.
     */
    @JsonCreator
    DurableHttpRequest(
            @JsonProperty("method") String method,
            @JsonProperty("uri") URI uri,
            @JsonProperty("headers") @Nullable Map<String, String> headers,
            @JsonProperty("content") @Nullable String content,
            @JsonProperty("tokenSource") @Nullable TokenSource tokenSource,
            @JsonProperty("asynchronousPatternEnabled") boolean asynchronousPatternEnabled,
            @JsonProperty("timeout") @Nullable String timeout,
            @JsonProperty("retryOptions") @Nullable HttpRetryOptions httpRetryOptions) {
        if (method == null || method.trim().isEmpty()) {
            throw new IllegalArgumentException("method must not be null or empty");
        }
        if (uri == null) {
            throw new IllegalArgumentException("uri must not be null");
        }
        this.method = method;
        this.uri = uri;
        this.headers = headers != null ? Collections.unmodifiableMap(new HashMap<>(headers)) : null;
        this.content = content;
        this.tokenSource = tokenSource;
        this.asynchronousPatternEnabled = asynchronousPatternEnabled;
        this.timeout = timeout;
        this.httpRetryOptions = httpRetryOptions;
    }

    /**
     * Gets the HTTP method for this request.
     *
     * @return the HTTP method (e.g., "GET", "POST", "PUT", "DELETE")
     */
    @JsonProperty("method")
    public String getMethod() {
        return this.method;
    }

    /**
     * Gets the target URI for this request.
     *
     * @return the target URI
     */
    @JsonProperty("uri")
    public URI getUri() {
        return this.uri;
    }

    /**
     * Gets the HTTP headers for this request.
     *
     * @return an unmodifiable map of HTTP headers, or {@code null} if no headers are set
     */
    @JsonProperty("headers")
    @Nullable
    public Map<String, String> getHeaders() {
        return this.headers;
    }

    /**
     * Gets the body content of this request.
     *
     * @return the body content, or {@code null} if no body is set
     */
    @JsonProperty("content")
    @Nullable
    public String getContent() {
        return this.content;
    }

    /**
     * Gets the token source for authentication.
     *
     * @return the token source, or {@code null} if no authentication is configured
     */
    @JsonProperty("tokenSource")
    @Nullable
    public TokenSource getTokenSource() {
        return this.tokenSource;
    }

    /**
     * Gets whether asynchronous HTTP 202 polling is enabled.
     * <p>
     * When enabled, if the target HTTP endpoint returns a 202 (Accepted) response with a {@code Location}
     * header, the framework will automatically poll the location URL until a non-202 response is received.
     *
     * @return {@code true} if asynchronous pattern is enabled, {@code false} otherwise
     */
    @JsonProperty("asynchronousPatternEnabled")
    public boolean isAsynchronousPatternEnabled() {
        return this.asynchronousPatternEnabled;
    }

    /**
     * Gets the total timeout for the HTTP request and any asynchronous polling.
     *
     * @return the timeout duration, or {@code null} if no timeout is set
     */
    @JsonIgnore
    @Nullable
    public Duration getTimeout() {
        return this.timeout != null ? TimeSpanHelper.parse(this.timeout) : null;
    }

    /**
     * Gets the timeout as a raw .NET TimeSpan format string for JSON serialization.
     *
     * @return the timeout string in .NET TimeSpan format, or {@code null} if no timeout is set
     */
    @JsonGetter("timeout")
    @Nullable
    String getTimeoutString() {
        return this.timeout;
    }

    /**
     * Gets the retry options for the HTTP request.
     *
     * @return the retry options, or {@code null} if no retries are configured
     */
    @JsonProperty("retryOptions")
    @Nullable
    public HttpRetryOptions getHttpRetryOptions() {
        return this.httpRetryOptions;
    }
}
