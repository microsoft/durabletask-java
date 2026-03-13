// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * A {@link TokenSource} implementation that uses Azure Managed Identity to acquire OAuth 2.0 access tokens.
 * <p>
 * When used with {@link DurableHttpRequest}, the Durable Functions host automatically acquires a token
 * from the Azure Managed Identity endpoint for the specified resource and attaches it as a bearer token
 * in the {@code Authorization} header of the outgoing HTTP request.
 * <p>
 * Token refresh is handled automatically by the framework. Tokens are never stored in the durable
 * orchestration state.
 * <p>
 * <b>Resource URI Auto-Normalization:</b> For well-known Azure resource base URIs
 * (e.g., {@code https://management.core.windows.net} and {@code https://graph.microsoft.com}),
 * if the provided resource URI does not end with {@code /.default}, it will be automatically appended.
 * This matches the .NET Durable Functions SDK behavior.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Simple usage (resource URI is auto-normalized to include /.default)
 * TokenSource tokenSource = new ManagedIdentityTokenSource("https://management.core.windows.net");
 *
 * // With explicit options
 * ManagedIdentityOptions options = new ManagedIdentityOptions(
 *     URI.create("https://login.microsoftonline.com/"), "my-tenant-id");
 * TokenSource tokenSource = new ManagedIdentityTokenSource(
 *     "https://management.core.windows.net/.default", options);
 *
 * DurableHttpRequest request = new DurableHttpRequest(
 *     "POST",
 *     new URI("https://management.azure.com/subscriptions/.../restart?api-version=2019-03-01"),
 *     null, null, tokenSource);
 * DurableHttpResponse response = DurableHttp.callHttp(ctx, request).await();
 * }</pre>
 *
 * @see TokenSource
 * @see ManagedIdentityOptions
 * @see DurableHttpRequest
 * @see DurableHttp
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties({"kind"})
public class ManagedIdentityTokenSource extends TokenSource {

    private static final String KIND = "AzureManagedIdentity";
    private static final String DEFAULT_SUFFIX = "/.default";

    /**
     * Well-known Azure resource base URIs that should have {@code /.default} appended
     * if not already present, matching the .NET SDK auto-normalization behavior.
     */
    private static final String[] KNOWN_RESOURCE_BASES = {
            "https://management.core.windows.net",
            "https://graph.microsoft.com"
    };

    private final String resource;
    private final ManagedIdentityOptions options;

    /**
     * Creates a new {@code ManagedIdentityTokenSource} for the specified resource.
     * <p>
     * If the resource URI matches a well-known Azure resource base URI and does not already
     * end with {@code /.default}, the suffix is automatically appended.
     *
     * @param resource the resource URI for which to acquire a token (e.g.,
     *                 {@code "https://management.core.windows.net/.default"} for Azure Resource Manager)
     */
    public ManagedIdentityTokenSource(String resource) {
        this(resource, null);
    }

    /**
     * Creates a new {@code ManagedIdentityTokenSource} for the specified resource with options.
     * <p>
     * If the resource URI matches a well-known Azure resource base URI and does not already
     * end with {@code /.default}, the suffix is automatically appended.
     *
     * @param resource the resource URI for which to acquire a token
     * @param options additional managed identity configuration options, or {@code null} for defaults
     */
    @JsonCreator
    public ManagedIdentityTokenSource(
            @JsonProperty("resource") String resource,
            @JsonProperty("options") @Nullable ManagedIdentityOptions options) {
        if (resource == null || resource.trim().isEmpty()) {
            throw new IllegalArgumentException("resource must not be null or empty");
        }
        this.resource = normalizeResource(resource);
        this.options = options;
    }

    @Override
    @JsonProperty("kind")
    public String getKind() {
        return KIND;
    }

    /**
     * Gets the resource URI for which tokens will be acquired.
     * <p>
     * If auto-normalization was applied, this returns the normalized URI (with {@code /.default} appended).
     *
     * @return the resource URI
     */
    @JsonProperty("resource")
    public String getResource() {
        return this.resource;
    }

    /**
     * Gets the managed identity configuration options.
     *
     * @return the options, or {@code null} if default options are used
     */
    @JsonProperty("options")
    @Nullable
    public ManagedIdentityOptions getOptions() {
        return this.options;
    }

    /**
     * Auto-normalizes well-known Azure resource URIs by appending {@code /.default} if not present.
     */
    private static String normalizeResource(String resource) {
        for (String base : KNOWN_RESOURCE_BASES) {
            if (resource.startsWith(base) && !resource.endsWith(DEFAULT_SUFFIX)) {
                return resource + DEFAULT_SUFFIX;
            }
        }
        return resource;
    }
}
