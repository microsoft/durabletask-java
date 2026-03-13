// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.net.URI;

/**
 * Configuration options for Azure Managed Identity token acquisition.
 * <p>
 * These options can be used with {@link ManagedIdentityTokenSource} to customize the authentication
 * behavior, such as specifying a custom Azure Active Directory authority host or a specific tenant ID.
 * <p>
 * Example usage:
 * <pre>{@code
 * ManagedIdentityOptions options = new ManagedIdentityOptions(
 *     URI.create("https://login.microsoftonline.com/"),
 *     "my-tenant-id");
 * TokenSource tokenSource = new ManagedIdentityTokenSource(
 *     "https://management.core.windows.net/.default", options);
 * }</pre>
 *
 * @see ManagedIdentityTokenSource
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ManagedIdentityOptions {

    private final URI authorityHost;
    private final String tenantId;

    /**
     * Creates a new {@code ManagedIdentityOptions} with the specified authority host and tenant ID.
     *
     * @param authorityHost the host of the Azure Active Directory authority
     *                      (e.g., {@code https://login.microsoftonline.com/}), or {@code null} for the default
     * @param tenantId the tenant ID of the user to authenticate, or {@code null} for the default tenant
     */
    @JsonCreator
    public ManagedIdentityOptions(
            @JsonProperty("authorityhost") @Nullable URI authorityHost,
            @JsonProperty("tenantid") @Nullable String tenantId) {
        this.authorityHost = authorityHost;
        this.tenantId = tenantId;
    }

    /**
     * Gets the host of the Azure Active Directory authority.
     * <p>
     * The default is {@code https://login.microsoftonline.com/}.
     *
     * @return the authority host URI, or {@code null} if the default should be used
     */
    @JsonProperty("authorityhost")
    @Nullable
    public URI getAuthorityHost() {
        return this.authorityHost;
    }

    /**
     * Gets the tenant ID of the user to authenticate.
     *
     * @return the tenant ID, or {@code null} if the default tenant should be used
     */
    @JsonProperty("tenantid")
    @Nullable
    public String getTenantId() {
        return this.tenantId;
    }
}
