// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Abstract base class for token sources used to authenticate durable HTTP requests.
 * <p>
 * Token sources are used with {@link DurableHttpRequest} to automatically acquire and attach
 * authentication tokens to outbound HTTP requests made by orchestrator functions.
 *
 * @see DurableHttpRequest
 * @see ManagedIdentityTokenSource
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ManagedIdentityTokenSource.class, name = "AzureManagedIdentity")
})
public abstract class TokenSource {

    /**
     * Gets the kind identifier for this token source.
     *
     * @return the token source kind string
     */
    @JsonProperty("kind")
    public abstract String getKind();
}
