// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link AccessTokenCache}.
 */
@ExtendWith(MockitoExtension.class)
public class AccessTokenCacheTest {

    @Mock
    private TokenCredential mockCredential;

    private TokenRequestContext context;
    private Duration margin;
    private AccessTokenCache tokenCache;

    @BeforeEach
    public void setup() {
        context = new TokenRequestContext().addScopes("https://durabletask.io/.default");
        margin = Duration.ofMinutes(5);
        tokenCache = new AccessTokenCache(mockCredential, context, margin);
    }

    @Test
    @DisplayName("getToken should fetch a new token when cache is empty")
    public void getToken_WhenCacheEmpty_FetchesNewToken() {
        // Arrange
        AccessToken expectedToken = new AccessToken("token1", OffsetDateTime.now().plusHours(1));
        when(mockCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.just(expectedToken));

        // Act
        AccessToken result = tokenCache.getToken();

        // Assert
        assertEquals(expectedToken, result);
        verify(mockCredential, times(1)).getToken(context);
    }

    @Test
    @DisplayName("getToken should reuse cached token when not expired")
    public void getToken_WhenTokenNotExpired_ReusesCachedToken() {
        // Arrange
        AccessToken expectedToken = new AccessToken("token1", OffsetDateTime.now().plusHours(1));
        when(mockCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.just(expectedToken));

        // Act
        tokenCache.getToken(); // First call to cache the token
        AccessToken result = tokenCache.getToken(); // Second call should use cached token

        // Assert
        assertEquals(expectedToken, result);
        verify(mockCredential, times(1)).getToken(context); // Should only be called once
    }

    @Test
    @DisplayName("getToken should fetch a new token when current token is expired")
    public void getToken_WhenTokenExpired_FetchesNewToken() {
        // Arrange
        AccessToken expiredToken = new AccessToken("expired", OffsetDateTime.now().minusMinutes(1));
        AccessToken newToken = new AccessToken("new", OffsetDateTime.now().plusHours(1));
        
        when(mockCredential.getToken(any(TokenRequestContext.class)))
            .thenReturn(Mono.just(expiredToken))
            .thenReturn(Mono.just(newToken));

        // Act
        AccessToken firstResult = tokenCache.getToken();
        AccessToken secondResult = tokenCache.getToken();

        // Assert
        assertEquals(expiredToken, firstResult);
        assertEquals(newToken, secondResult);
        verify(mockCredential, times(2)).getToken(context);
    }

    @Test
    @DisplayName("getToken should fetch a new token when current token is about to expire within margin")
    public void getToken_WhenTokenAboutToExpire_FetchesNewToken() {
        // Arrange
        AccessToken expiringToken = new AccessToken("expiring", OffsetDateTime.now().plus(margin.minusMinutes(1)));
        AccessToken newToken = new AccessToken("new", OffsetDateTime.now().plusHours(1));
        
        when(mockCredential.getToken(any(TokenRequestContext.class)))
            .thenReturn(Mono.just(expiringToken))
            .thenReturn(Mono.just(newToken));

        // Act
        AccessToken firstResult = tokenCache.getToken();
        AccessToken secondResult = tokenCache.getToken();

        // Assert
        assertEquals(expiringToken, firstResult);
        assertEquals(newToken, secondResult);
        verify(mockCredential, times(2)).getToken(context);
    }
}
