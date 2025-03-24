// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;

import java.time.Duration;
import java.time.OffsetDateTime;

/**
 * Caches access tokens for Azure authentication.
 * This class is used by both client and worker components to authenticate with Azure-managed Durable Task Scheduler.
 */
public final class AccessTokenCache {
    private final TokenCredential credential;
    private final TokenRequestContext context;
    private final Duration margin;
    private AccessToken cachedToken;

    /**
     * Creates a new instance of the AccessTokenCache.
     * 
     * @param credential The token credential to use for obtaining tokens.
     * @param context The token request context specifying the scopes.
     * @param margin The time margin before token expiration to refresh the token.
     */
    public AccessTokenCache(TokenCredential credential, TokenRequestContext context, Duration margin) {
        this.credential = credential;
        this.context = context;
        this.margin = margin;
    }

    /**
     * Gets a valid access token, refreshing it if necessary.
     * 
     * @return A valid access token.
     */
    public AccessToken getToken() {
        OffsetDateTime nowWithMargin = OffsetDateTime.now().plus(margin);

        if (cachedToken == null 
            || cachedToken.getExpiresAt().isBefore(nowWithMargin)) {
            this.cachedToken = credential.getToken(context).block();
        }

        return cachedToken;
    }
} 