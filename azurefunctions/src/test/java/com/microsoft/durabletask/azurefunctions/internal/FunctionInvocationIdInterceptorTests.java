// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions.internal;

import io.grpc.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for FunctionInvocationIdInterceptor.
 */
public class FunctionInvocationIdInterceptorTests {

    private static final Metadata.Key<String> INVOCATION_ID_KEY = 
        Metadata.Key.of("x-azure-functions-invocationid", Metadata.ASCII_STRING_MARSHALLER);

    @Test
    public void interceptCall_addsInvocationIdToMetadata() {
        // Arrange
        String testInvocationId = "test-invocation-id-123";
        FunctionInvocationIdInterceptor interceptor = new FunctionInvocationIdInterceptor(testInvocationId);
        
        Channel mockChannel = mock(Channel.class);
        ClientCall<Object, Object> mockCall = mock(ClientCall.class);
        MethodDescriptor<Object, Object> mockMethod = mock(MethodDescriptor.class);
        CallOptions callOptions = CallOptions.DEFAULT;
        
        when(mockChannel.newCall(any(), any())).thenReturn(mockCall);
        
        // Act
        ClientCall<Object, Object> interceptedCall = interceptor.interceptCall(mockMethod, callOptions, mockChannel);
        
        // Assert - Start the call to trigger the metadata modification
        Metadata headers = new Metadata();
        interceptedCall.start(mock(ClientCall.Listener.class), headers);
        
        // Verify the invocation ID was added to the headers
        assertEquals(testInvocationId, headers.get(INVOCATION_ID_KEY));
    }

    @Test
    public void interceptCall_withNullInvocationId_doesNotAddHeader() {
        // Arrange
        FunctionInvocationIdInterceptor interceptor = new FunctionInvocationIdInterceptor(null);
        
        Channel mockChannel = mock(Channel.class);
        ClientCall<Object, Object> mockCall = mock(ClientCall.class);
        MethodDescriptor<Object, Object> mockMethod = mock(MethodDescriptor.class);
        CallOptions callOptions = CallOptions.DEFAULT;
        
        when(mockChannel.newCall(any(), any())).thenReturn(mockCall);
        
        // Act
        ClientCall<Object, Object> interceptedCall = interceptor.interceptCall(mockMethod, callOptions, mockChannel);
        
        // Assert - Start the call to trigger the metadata modification
        Metadata headers = new Metadata();
        interceptedCall.start(mock(ClientCall.Listener.class), headers);
        
        // Verify no invocation ID was added
        assertNull(headers.get(INVOCATION_ID_KEY));
    }

    @Test
    public void interceptCall_withEmptyInvocationId_doesNotAddHeader() {
        // Arrange
        FunctionInvocationIdInterceptor interceptor = new FunctionInvocationIdInterceptor("");
        
        Channel mockChannel = mock(Channel.class);
        ClientCall<Object, Object> mockCall = mock(ClientCall.class);
        MethodDescriptor<Object, Object> mockMethod = mock(MethodDescriptor.class);
        CallOptions callOptions = CallOptions.DEFAULT;
        
        when(mockChannel.newCall(any(), any())).thenReturn(mockCall);
        
        // Act
        ClientCall<Object, Object> interceptedCall = interceptor.interceptCall(mockMethod, callOptions, mockChannel);
        
        // Assert - Start the call to trigger the metadata modification
        Metadata headers = new Metadata();
        interceptedCall.start(mock(ClientCall.Listener.class), headers);
        
        // Verify no invocation ID was added
        assertNull(headers.get(INVOCATION_ID_KEY));
    }

    @Test
    public void constructor_acceptsValidInvocationId() {
        // Act & Assert - no exception should be thrown
        FunctionInvocationIdInterceptor interceptor = new FunctionInvocationIdInterceptor("valid-id");
        assertNotNull(interceptor);
    }

    @Test
    public void constructor_acceptsNull() {
        // Act & Assert - no exception should be thrown
        FunctionInvocationIdInterceptor interceptor = new FunctionInvocationIdInterceptor(null);
        assertNotNull(interceptor);
    }
}
