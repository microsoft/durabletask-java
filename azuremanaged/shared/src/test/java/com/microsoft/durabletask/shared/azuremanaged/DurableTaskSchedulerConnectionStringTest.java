package com.microsoft.durabletask.shared.azuremanaged;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DurableTaskSchedulerConnectionString}.
 */
public class DurableTaskSchedulerConnectionStringTest {

    private static final String VALID_CONNECTION_STRING = 
        "Endpoint=https://example.com;Authentication=ManagedIdentity;TaskHub=myTaskHub";
    
    @Test
    @DisplayName("Constructor should parse valid connection string")
    public void constructor_ParsesValidConnectionString() {
        // Arrange & Act
        DurableTaskSchedulerConnectionString connectionString = 
            new DurableTaskSchedulerConnectionString(VALID_CONNECTION_STRING);
        
        // Assert
        assertEquals("https://example.com", connectionString.getEndpoint());
        assertEquals("ManagedIdentity", connectionString.getAuthentication());
        assertEquals("myTaskHub", connectionString.getTaskHubName());
    }
    
    @Test
    @DisplayName("Constructor should handle connection string with whitespace")
    public void constructor_HandlesWhitespace() {
        // Arrange
        String connectionStringWithSpaces = 
            "Endpoint = https://example.com ; Authentication = ManagedIdentity ; TaskHub = myTaskHub";
        
        // Act
        DurableTaskSchedulerConnectionString connectionString = 
            new DurableTaskSchedulerConnectionString(connectionStringWithSpaces);
        
        // Assert
        assertEquals("https://example.com", connectionString.getEndpoint());
        assertEquals("ManagedIdentity", connectionString.getAuthentication());
        assertEquals("myTaskHub", connectionString.getTaskHubName());
    }
    
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    @DisplayName("Constructor should throw for null or empty connection string")
    public void constructor_ThrowsForNullOrEmptyConnectionString(String invalidInput) {
        // Act & Assert
        assertThrows(IllegalArgumentException.class, 
            () -> new DurableTaskSchedulerConnectionString(invalidInput));
    }
    
    @Test
    @DisplayName("Constructor should throw when missing required Endpoint property")
    public void constructor_ThrowsWhenMissingEndpoint() {
        // Arrange
        String missingEndpoint = "Authentication=ManagedIdentity;TaskHub=myTaskHub";
        
        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
            () -> new DurableTaskSchedulerConnectionString(missingEndpoint));
        
        assertTrue(exception.getMessage().contains("Endpoint"));
    }
    
    @Test
    @DisplayName("Constructor should throw when missing required Authentication property")
    public void constructor_ThrowsWhenMissingAuthentication() {
        // Arrange
        String missingAuthentication = "Endpoint=https://example.com;TaskHub=myTaskHub";
        
        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
            () -> new DurableTaskSchedulerConnectionString(missingAuthentication));
        
        assertTrue(exception.getMessage().contains("Authentication"));
    }
    
    @Test
    @DisplayName("Constructor should throw when missing required TaskHub property")
    public void constructor_ThrowsWhenMissingTaskHub() {
        // Arrange
        String missingTaskHub = "Endpoint=https://example.com;Authentication=ManagedIdentity";
        
        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
            () -> new DurableTaskSchedulerConnectionString(missingTaskHub));
        
        assertTrue(exception.getMessage().contains("TaskHub"));
    }
    
    @Test
    @DisplayName("getAdditionallyAllowedTenants should return split comma-separated values")
    public void getAdditionallyAllowedTenants_ShouldSplitCommaValues() {
        // Arrange
        String connectionString = VALID_CONNECTION_STRING + 
            ";AdditionallyAllowedTenants=tenant1,tenant2,tenant3";
        
        // Act
        DurableTaskSchedulerConnectionString parsedString = 
            new DurableTaskSchedulerConnectionString(connectionString);
        List<String> tenants = parsedString.getAdditionallyAllowedTenants();
        
        // Assert
        assertNotNull(tenants);
        assertEquals(3, tenants.size());
        assertEquals("tenant1", tenants.get(0));
        assertEquals("tenant2", tenants.get(1));
        assertEquals("tenant3", tenants.get(2));
    }
    
    @Test
    @DisplayName("getAdditionallyAllowedTenants should return null when property not present")
    public void getAdditionallyAllowedTenants_ReturnsNullWhenNotPresent() {
        // Arrange & Act
        DurableTaskSchedulerConnectionString connectionString = 
            new DurableTaskSchedulerConnectionString(VALID_CONNECTION_STRING);
        
        // Assert
        assertNull(connectionString.getAdditionallyAllowedTenants());
    }
    
    @Test
    @DisplayName("getClientId should return correct value when present")
    public void getClientId_ReturnsValueWhenPresent() {
        // Arrange
        String connectionString = VALID_CONNECTION_STRING + ";ClientID=my-client-id";
        
        // Act
        DurableTaskSchedulerConnectionString parsedString = 
            new DurableTaskSchedulerConnectionString(connectionString);
        
        // Assert
        assertEquals("my-client-id", parsedString.getClientId());
    }
    
    @Test
    @DisplayName("getTenantId should return correct value when present")
    public void getTenantId_ReturnsValueWhenPresent() {
        // Arrange
        String connectionString = VALID_CONNECTION_STRING + ";TenantId=my-tenant-id";
        
        // Act
        DurableTaskSchedulerConnectionString parsedString = 
            new DurableTaskSchedulerConnectionString(connectionString);
        
        // Assert
        assertEquals("my-tenant-id", parsedString.getTenantId());
    }
    
    @Test
    @DisplayName("getTokenFilePath should return correct value when present")
    public void getTokenFilePath_ReturnsValueWhenPresent() {
        // Arrange
        String connectionString = VALID_CONNECTION_STRING + ";TokenFilePath=/path/to/token";
        
        // Act
        DurableTaskSchedulerConnectionString parsedString = 
            new DurableTaskSchedulerConnectionString(connectionString);
        
        // Assert
        assertEquals("/path/to/token", parsedString.getTokenFilePath());
    }
} 