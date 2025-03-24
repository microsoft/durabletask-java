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
 * Unit tests for {@link DurableTaskSchedulerClientOptions}.
 */
@ExtendWith(MockitoExtension.class)
public class DurableTaskSchedulerClientOptionsTest {

    private static final String VALID_CONNECTION_STRING = 
        "Endpoint=https://example.com;Authentication=ManagedIdentity;TaskHub=myTaskHub";
    private static final String VALID_ENDPOINT = "https://example.com";
    private static final String VALID_TASKHUB = "myTaskHub";
    private static final String CUSTOM_RESOURCE_ID = "https://custom.resource";
    private static final Duration CUSTOM_REFRESH_MARGIN = Duration.ofMinutes(10);

    @Mock
    private TokenCredential mockCredential;

    @Test
    @DisplayName("fromConnectionString should create valid options")
    public void fromConnectionString_CreatesValidOptions() {
        // Act
        DurableTaskSchedulerClientOptions options = 
            DurableTaskSchedulerClientOptions.fromConnectionString(VALID_CONNECTION_STRING);

        // Assert
        assertNotNull(options);
        assertEquals(VALID_ENDPOINT, options.getEndpointAddress());
        assertEquals(VALID_TASKHUB, options.getTaskHubName());
    }

    @Test
    @DisplayName("setEndpointAddress should update endpoint")
    public void setEndpointAddress_UpdatesEndpoint() {
        // Arrange
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions();

        // Act
        options.setEndpointAddress(VALID_ENDPOINT);

        // Assert
        assertEquals(VALID_ENDPOINT, options.getEndpointAddress());
    }

    @Test
    @DisplayName("setTaskHubName should update task hub name")
    public void setTaskHubName_UpdatesTaskHubName() {
        // Arrange
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions();

        // Act
        options.setTaskHubName(VALID_TASKHUB);

        // Assert
        assertEquals(VALID_TASKHUB, options.getTaskHubName());
    }

    @Test
    @DisplayName("setCredential should update credential")
    public void setCredential_UpdatesCredential() {
        // Arrange
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions();

        // Act
        options.setCredential(mockCredential);

        // Assert
        assertEquals(mockCredential, options.getCredential());
    }

    @Test
    @DisplayName("setResourceId should update resource ID")
    public void setResourceId_UpdatesResourceId() {
        // Arrange
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions();

        // Act
        options.setResourceId(CUSTOM_RESOURCE_ID);

        // Assert
        assertEquals(CUSTOM_RESOURCE_ID, options.getResourceId());
    }

    @Test
    @DisplayName("setAllowInsecureCredentials should update insecure credentials flag")
    public void setAllowInsecureCredentials_UpdatesFlag() {
        // Arrange
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions();

        // Act
        options.setAllowInsecureCredentials(true);

        // Assert
        assertTrue(options.isAllowInsecureCredentials());
    }

    @Test
    @DisplayName("setTokenRefreshMargin should update token refresh margin")
    public void setTokenRefreshMargin_UpdatesMargin() {
        // Arrange
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions();

        // Act
        options.setTokenRefreshMargin(CUSTOM_REFRESH_MARGIN);

        // Assert
        assertEquals(CUSTOM_REFRESH_MARGIN, options.getTokenRefreshMargin());
    }

    @Test
    @DisplayName("createGrpcChannel should create valid channel")
    public void createGrpcChannel_CreatesValidChannel() {
        // Arrange
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions()
            .setEndpointAddress(VALID_ENDPOINT)
            .setTaskHubName(VALID_TASKHUB);

        // Act
        Channel channel = options.createGrpcChannel();

        // Assert
        assertNotNull(channel);
    }

    @Test
    @DisplayName("createGrpcChannel should handle endpoint without protocol")
    public void createGrpcChannel_HandlesEndpointWithoutProtocol() {
        // Arrange
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions()
            .setEndpointAddress("example.com")
            .setTaskHubName(VALID_TASKHUB);

        // Act
        Channel channel = options.createGrpcChannel();

        // Assert
        assertNotNull(channel);
    }
} 