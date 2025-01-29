// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;

import java.time.Duration;
import java.time.OffsetDateTime;

public final class AccessTokenCache {
    private final TokenCredential credential;
    private final TokenRequestContext context;
    private final Duration margin;
    private AccessToken cachedToken;

    public AccessTokenCache(TokenCredential credential, TokenRequestContext context, Duration margin) {
        this.credential = credential;
        this.context = context;
        this.margin = margin;
    }

    public AccessToken getToken() {
        OffsetDateTime nowWithMargin = OffsetDateTime.now().plus(margin);

        if (cachedToken == null 
            || cachedToken.getExpiresAt().isBefore(nowWithMargin)) {
            this.cachedToken = credential.getToken(context).block();
        }

        return cachedToken;
    }
}