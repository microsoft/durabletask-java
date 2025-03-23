package com.microsoft.durabletask.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import io.grpc.Channel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link DurableTaskSchedulerClientExtensions}.
 */
@ExtendWith(MockitoExtension.class)
public class DurableTaskSchedulerClientExtensionsTest {

    private static final String VALID_CONNECTION_STRING = 
        "Endpoint=https://example.com;Authentication=ManagedIdentity;TaskHub=myTaskHub";
    private static final String VALID_ENDPOINT = "https://example.com";
    private static final String VALID_TASKHUB = "myTaskHub";

    @Mock
    private DurableTaskGrpcClientBuilder mockBuilder;
    
    @Mock
    private TokenCredential mockCredential;
    
    @Test
    @DisplayName("useDurableTaskScheduler with connection string should configure builder correctly")
    public void useDurableTaskScheduler_WithConnectionString_ConfiguresBuilder() {
        // Arrange
        when(mockBuilder.grpcChannel(any(Channel.class))).thenReturn(mockBuilder);
        
        // Act
        DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(
            mockBuilder, VALID_CONNECTION_STRING);
        
        // Assert
        verify(mockBuilder).grpcChannel(any(Channel.class));
    }
    
    @Test
    @DisplayName("useDurableTaskScheduler with connection string should throw for null builder")
    public void useDurableTaskScheduler_WithConnectionString_ThrowsForNullBuilder() {
        // Act & Assert
        assertThrows(NullPointerException.class, 
            () -> DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(
                null, VALID_CONNECTION_STRING));
    }
    
    @Test
    @DisplayName("useDurableTaskScheduler with connection string should throw for null connection string")
    public void useDurableTaskScheduler_WithConnectionString_ThrowsForNullConnectionString() {
        // Act & Assert
        assertThrows(NullPointerException.class, 
            () -> DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(
                mockBuilder, null));
    }
    
    @Test
    @DisplayName("useDurableTaskScheduler with explicit parameters should configure builder correctly")
    public void useDurableTaskScheduler_WithExplicitParameters_ConfiguresBuilder() {
        // Arrange
        when(mockBuilder.grpcChannel(any(Channel.class))).thenReturn(mockBuilder);
        
        // Act
        DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(
            mockBuilder, VALID_ENDPOINT, VALID_TASKHUB, mockCredential);
        
        // Assert
        verify(mockBuilder).grpcChannel(any(Channel.class));
    }
    
    @Test
    @DisplayName("useDurableTaskScheduler with explicit parameters should throw for null builder")
    public void useDurableTaskScheduler_WithExplicitParameters_ThrowsForNullBuilder() {
        // Act & Assert
        assertThrows(NullPointerException.class, 
            () -> DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(
                null, VALID_ENDPOINT, VALID_TASKHUB, mockCredential));
    }
    
    @Test
    @DisplayName("useDurableTaskScheduler with explicit parameters should throw for null endpoint")
    public void useDurableTaskScheduler_WithExplicitParameters_ThrowsForNullEndpoint() {
        // Act & Assert
        assertThrows(NullPointerException.class, 
            () -> DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(
                mockBuilder, null, VALID_TASKHUB, mockCredential));
    }
    
    @Test
    @DisplayName("useDurableTaskScheduler with explicit parameters should throw for null task hub name")
    public void useDurableTaskScheduler_WithExplicitParameters_ThrowsForNullTaskHubName() {
        // Act & Assert
        assertThrows(NullPointerException.class, 
            () -> DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(
                mockBuilder, VALID_ENDPOINT, null, mockCredential));
    }
    
    @Test
    @DisplayName("createClientBuilder with connection string should create valid builder")
    public void createClientBuilder_WithConnectionString_CreatesValidBuilder() {
        // Act
        DurableTaskGrpcClientBuilder result = 
            DurableTaskSchedulerClientExtensions.createClientBuilder(VALID_CONNECTION_STRING);
        
        // Assert
        assertNotNull(result);
    }
    
    @Test
    @DisplayName("createClientBuilder with connection string should throw for null connection string")
    public void createClientBuilder_WithConnectionString_ThrowsForNullConnectionString() {
        // Act & Assert
        assertThrows(NullPointerException.class, 
            () -> DurableTaskSchedulerClientExtensions.createClientBuilder(null));
    }
    
    @Test
    @DisplayName("createClientBuilder with explicit parameters should create valid builder")
    public void createClientBuilder_WithExplicitParameters_CreatesValidBuilder() {
        // Act
        DurableTaskGrpcClientBuilder result = 
            DurableTaskSchedulerClientExtensions.createClientBuilder(
                VALID_ENDPOINT, VALID_TASKHUB, mockCredential);
        
        // Assert
        assertNotNull(result);
    }
    
    @Test
    @DisplayName("createClientBuilder with explicit parameters should throw for null endpoint")
    public void createClientBuilder_WithExplicitParameters_ThrowsForNullEndpoint() {
        // Act & Assert
        assertThrows(NullPointerException.class, 
            () -> DurableTaskSchedulerClientExtensions.createClientBuilder(
                null, VALID_TASKHUB, mockCredential));
    }
    
    @Test
    @DisplayName("createClientBuilder with explicit parameters should throw for null task hub name")
    public void createClientBuilder_WithExplicitParameters_ThrowsForNullTaskHubName() {
        // Act & Assert
        assertThrows(NullPointerException.class, 
            () -> DurableTaskSchedulerClientExtensions.createClientBuilder(
                VALID_ENDPOINT, null, mockCredential));
    }
} 