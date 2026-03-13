// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

/**
 * Represents an HTTP response returned by a durable HTTP call made via
 * {@link DurableHttp#callHttp}.
 * <p>
 * Instances of this class are created by the Durable Functions host when the HTTP call completes.
 * The response data, including status code, headers, and content, is durably persisted in the
 * orchestration history.
 * <p>
 * Example usage:
 * <pre>{@code
 * DurableHttpResponse response = DurableHttp.callHttp(ctx, request).await();
 * int statusCode = response.getStatusCode();
 * String body = response.getContent();
 * }</pre>
 *
 * @see DurableHttpRequest
 * @see DurableHttp#callHttp
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DurableHttpResponse {

    private final int statusCode;
    private final Map<String, String> headers;
    private final String content;

    /**
     * Creates a new {@code DurableHttpResponse}.
     *
     * @param statusCode the HTTP status code of the response
     * @param headers the HTTP response headers, or {@code null} if no headers are present
     * @param content the body content of the response, or {@code null} if empty
     */
    @JsonCreator
    public DurableHttpResponse(
            @JsonProperty("statusCode") int statusCode,
            @JsonProperty("headers") @Nullable Map<String, String> headers,
            @JsonProperty("content") @Nullable String content) {
        this.statusCode = statusCode;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : null;
        this.content = content;
    }

    /**
     * Gets the HTTP status code of the response.
     *
     * @return the HTTP status code (e.g., 200, 404, 500)
     */
    @JsonProperty("statusCode")
    public int getStatusCode() {
        return this.statusCode;
    }

    /**
     * Gets the HTTP response headers.
     *
     * @return an unmodifiable map of response headers, or {@code null} if no headers are present
     */
    @JsonProperty("headers")
    @Nullable
    public Map<String, String> getHeaders() {
        return this.headers;
    }

    /**
     * Gets the body content of the response.
     *
     * @return the body content as a string, or {@code null} if the response has no body
     */
    @JsonProperty("content")
    @Nullable
    public String getContent() {
        return this.content;
    }
}
