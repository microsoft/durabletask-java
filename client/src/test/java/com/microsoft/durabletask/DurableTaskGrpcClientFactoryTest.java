// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DurableTaskGrpcClientFactory.
 */
public class DurableTaskGrpcClientFactoryTest {

    @Test
    void getClient_samePort_returnsSameInstance() {
        // Arrange
        int port = 5001;

        // Act
        DurableTaskClient client1 = DurableTaskGrpcClientFactory.getClient(port);
        DurableTaskClient client2 = DurableTaskGrpcClientFactory.getClient(port);

        // Assert
        assertNotNull(client1, "First client should not be null");
        assertNotNull(client2, "Second client should not be null");
        assertSame(client1, client2, "getClient should return the same instance for the same port");
    }

    @Test
    void getClient_differentPorts_returnsDifferentInstances() {
        // Arrange
        int port1 = 5002;
        int port2 = 5003;

        // Act
        DurableTaskClient client1 = DurableTaskGrpcClientFactory.getClient(port1);
        DurableTaskClient client2 = DurableTaskGrpcClientFactory.getClient(port2);

        // Assert
        assertNotNull(client1, "Client for port1 should not be null");
        assertNotNull(client2, "Client for port2 should not be null");
        assertNotSame(client1, client2, "getClient should return different instances for different ports");
    }

    @Test
    void getClient_multiplePorts_maintainsCorrectMapping() {
        // Arrange
        int port1 = 5004;
        int port2 = 5005;
        int port3 = 5006;

        // Act
        DurableTaskClient client1 = DurableTaskGrpcClientFactory.getClient(port1);
        DurableTaskClient client2 = DurableTaskGrpcClientFactory.getClient(port2);
        DurableTaskClient client3 = DurableTaskGrpcClientFactory.getClient(port3);
        
        // Request the same ports again
        DurableTaskClient client1Again = DurableTaskGrpcClientFactory.getClient(port1);
        DurableTaskClient client2Again = DurableTaskGrpcClientFactory.getClient(port2);
        DurableTaskClient client3Again = DurableTaskGrpcClientFactory.getClient(port3);

        // Assert
        // Verify each port returns the same instance
        assertSame(client1, client1Again, "Port " + port1 + " should return the same instance");
        assertSame(client2, client2Again, "Port " + port2 + " should return the same instance");
        assertSame(client3, client3Again, "Port " + port3 + " should return the same instance");
        
        // Verify all instances are different from each other
        assertNotSame(client1, client2, "Client for port1 and port2 should be different");
        assertNotSame(client1, client3, "Client for port1 and port3 should be different");
        assertNotSame(client2, client3, "Client for port2 and port3 should be different");
    }
}
