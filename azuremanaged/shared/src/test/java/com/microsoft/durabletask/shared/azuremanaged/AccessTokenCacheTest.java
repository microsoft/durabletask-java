package com.microsoft.durabletask.shared.azuremanaged;

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

    private static final Duration TOKEN_REFRESH_MARGIN = Duration.ofMinutes(5);
    private static final String RESOURCE_ID = "https://durabletask.io";

    @Mock
    private TokenCredential mockCredential;

    private TokenRequestContext expectedContext;
    private AccessTokenCache tokenCache;

    @BeforeEach
    public void setup() {
        expectedContext = new TokenRequestContext();
        expectedContext.addScopes(new String[] { RESOURCE_ID + "/.default" });
        tokenCache = new AccessTokenCache(mockCredential, expectedContext, TOKEN_REFRESH_MARGIN);
    }

    @Test
    @DisplayName("getToken should request token when cache is empty")
    public void getToken_RequestsTokenWhenCacheEmpty() {
        // Arrange
        OffsetDateTime expiresAt = OffsetDateTime.now().plusHours(1);
        AccessToken expectedToken = new AccessToken("token-value", expiresAt);
        when(mockCredential.getToken(any())).thenReturn(Mono.just(expectedToken));

        // Act
        AccessToken result = tokenCache.getToken();

        // Assert
        assertNotNull(result);
        assertEquals("token-value", result.getToken());
        assertEquals(expiresAt, result.getExpiresAt());
        verify(mockCredential).getToken(expectedContext);
    }

    @Test
    @DisplayName("getToken should return cached token when not expired")
    public void getToken_ReturnsCachedTokenWhenNotExpired() {
        // Arrange
        OffsetDateTime expiresAt = OffsetDateTime.now().plusHours(1);
        AccessToken expectedToken = new AccessToken("token-value", expiresAt);
        when(mockCredential.getToken(any())).thenReturn(Mono.just(expectedToken));

        // Act - First call to populate cache
        tokenCache.getToken();
        
        // Act - Second call should use cache
        AccessToken result = tokenCache.getToken();

        // Assert
        assertNotNull(result);
        assertEquals("token-value", result.getToken());
        assertEquals(expiresAt, result.getExpiresAt());
        // Verify token was only requested once
        verify(mockCredential, times(1)).getToken(any());
    }

    @Test
    @DisplayName("getToken should request new token when existing token is about to expire")
    public void getToken_RequestsNewTokenWhenExpiring() {
        // Arrange
        // First token expires soon (within refresh margin)
        OffsetDateTime expiryOne = OffsetDateTime.now().plus(TOKEN_REFRESH_MARGIN.dividedBy(2));
        AccessToken tokenOne = new AccessToken("token-one", expiryOne);
        
        // Second token expires much later
        OffsetDateTime expiryTwo = OffsetDateTime.now().plusHours(1);
        AccessToken tokenTwo = new AccessToken("token-two", expiryTwo);
        
        // Setup mock to return different tokens on each call
        when(mockCredential.getToken(any()))
            .thenReturn(Mono.just(tokenOne))
            .thenReturn(Mono.just(tokenTwo));

        // Act - First call to populate cache
        AccessToken firstResult = tokenCache.getToken();
        
        // Act - Second call should request new token due to expiration
        AccessToken secondResult = tokenCache.getToken();

        // Assert
        assertNotNull(firstResult);
        assertEquals("token-one", firstResult.getToken());
        
        assertNotNull(secondResult);
        assertEquals("token-two", secondResult.getToken());
        
        // Verify token was requested twice
        verify(mockCredential, times(2)).getToken(any());
    }

    @Test
    @DisplayName("getToken should handle null values properly")
    public void getToken_HandlesNullValuesProperly() {
        // Arrange
        when(mockCredential.getToken(any())).thenReturn(Mono.empty());

        // Act & Assert
        assertThrows(NullPointerException.class, () -> tokenCache.getToken());
    }
} 