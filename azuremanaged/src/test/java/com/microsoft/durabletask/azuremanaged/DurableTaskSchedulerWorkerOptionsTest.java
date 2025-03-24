package com.microsoft.durabletask.azuremanaged;

import com.azure.core.credential.TokenCredential;
import io.grpc.Channel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DurableTaskSchedulerWorkerOptions}.
 */
@ExtendWith(MockitoExtension.class)
public class DurableTaskSchedulerWorkerOptionsTest {

    private static final String VALID_ENDPOINT = "https://example.com";
    private static final String VALID_TASKHUB = "myTaskHub";
    private static final String VALID_CONNECTION_STRING = 
        "Endpoint=https://example.com;Authentication=ManagedIdentity;TaskHub=myTaskHub";

    @Mock
    private TokenCredential mockCredential;

    @Test
    @DisplayName("Default constructor should set default values")
    public void defaultConstructor_SetsDefaultValues() {
        // Act
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions();
        
        // Assert
        assertEquals("", options.getEndpointAddress());
        assertEquals("", options.getTaskHubName());
        assertNull(options.getCredential());
        assertEquals("https://durabletask.io", options.getResourceId());
        assertFalse(options.isAllowInsecureCredentials());
        assertEquals(Duration.ofMinutes(5), options.getTokenRefreshMargin());
    }

    @Test
    @DisplayName("fromConnectionString should parse connection string correctly")
    public void fromConnectionString_ParsesConnectionStringCorrectly() {
        // Act
        DurableTaskSchedulerWorkerOptions options = 
            DurableTaskSchedulerWorkerOptions.fromConnectionString(VALID_CONNECTION_STRING);
        
        // Assert
        assertEquals(VALID_ENDPOINT, options.getEndpointAddress());
        assertEquals(VALID_TASKHUB, options.getTaskHubName());
        assertNotNull(options.getCredential());
        assertTrue(options.getCredential() instanceof com.azure.identity.ManagedIdentityCredential);
        assertFalse(options.isAllowInsecureCredentials());
    }

    @Test
    @DisplayName("fromConnectionString should allow insecure credentials when authentication is None")
    public void fromConnectionString_AllowsInsecureCredentialsWhenAuthenticationIsNone() {
        // Arrange
        String connectionString = "Endpoint=https://example.com;Authentication=None;TaskHub=myTaskHub";
        
        // Act
        DurableTaskSchedulerWorkerOptions options = 
            DurableTaskSchedulerWorkerOptions.fromConnectionString(connectionString);
        
        // Assert
        assertTrue(options.isAllowInsecureCredentials());
        assertNull(options.getCredential());
    }

    @Test
    @DisplayName("setEndpointAddress should update endpoint address")
    public void setEndpointAddress_UpdatesEndpointAddress() {
        // Arrange
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions();
        
        // Act
        DurableTaskSchedulerWorkerOptions result = options.setEndpointAddress(VALID_ENDPOINT);
        
        // Assert
        assertEquals(VALID_ENDPOINT, options.getEndpointAddress());
        assertSame(options, result); // Builder pattern returns this
    }

    @Test
    @DisplayName("setTaskHubName should update task hub name")
    public void setTaskHubName_UpdatesTaskHubName() {
        // Arrange
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions();
        
        // Act
        DurableTaskSchedulerWorkerOptions result = options.setTaskHubName(VALID_TASKHUB);
        
        // Assert
        assertEquals(VALID_TASKHUB, options.getTaskHubName());
        assertSame(options, result); // Builder pattern returns this
    }

    @Test
    @DisplayName("setCredential should update credential")
    public void setCredential_UpdatesCredential() {
        // Arrange
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions();
        
        // Act
        DurableTaskSchedulerWorkerOptions result = options.setCredential(mockCredential);
        
        // Assert
        assertSame(mockCredential, options.getCredential());
        assertSame(options, result); // Builder pattern returns this
    }

    @Test
    @DisplayName("setResourceId should update resource ID")
    public void setResourceId_UpdatesResourceId() {
        // Arrange
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions();
        String customResourceId = "https://custom-resource.example.com";
        
        // Act
        DurableTaskSchedulerWorkerOptions result = options.setResourceId(customResourceId);
        
        // Assert
        assertEquals(customResourceId, options.getResourceId());
        assertSame(options, result); // Builder pattern returns this
    }

    @Test
    @DisplayName("setAllowInsecureCredentials should update allowInsecureCredentials")
    public void setAllowInsecureCredentials_UpdatesAllowInsecureCredentials() {
        // Arrange
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions();
        
        // Act
        DurableTaskSchedulerWorkerOptions result = options.setAllowInsecureCredentials(true);
        
        // Assert
        assertTrue(options.isAllowInsecureCredentials());
        assertSame(options, result); // Builder pattern returns this
    }

    @Test
    @DisplayName("setTokenRefreshMargin should update token refresh margin")
    public void setTokenRefreshMargin_UpdatesTokenRefreshMargin() {
        // Arrange
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions();
        Duration customMargin = Duration.ofMinutes(10);
        
        // Act
        DurableTaskSchedulerWorkerOptions result = options.setTokenRefreshMargin(customMargin);
        
        // Assert
        assertEquals(customMargin, options.getTokenRefreshMargin());
        assertSame(options, result); // Builder pattern returns this
    }

    @Test
    @DisplayName("createGrpcChannel should create a channel")
    public void createGrpcChannel_CreatesChannel() {
        // Arrange
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress(VALID_ENDPOINT)
            .setTaskHubName(VALID_TASKHUB);
        
        // Act
        Channel channel = options.createGrpcChannel();
        
        // Assert
        assertNotNull(channel);
    }

    @Test
    @DisplayName("createGrpcChannel should handle endpoints without protocol")
    public void createGrpcChannel_HandlesEndpointsWithoutProtocol() {
        // Arrange
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress("example.com")
            .setTaskHubName(VALID_TASKHUB);
        
        // Act & Assert
        assertDoesNotThrow(() -> options.createGrpcChannel());
    }

    @Test
    @DisplayName("createGrpcChannel should throw for invalid URLs")
    public void createGrpcChannel_ThrowsForInvalidUrls() {
        // Arrange
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress("invalid:url")
            .setTaskHubName(VALID_TASKHUB);
        
        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> options.createGrpcChannel());
    }
} 