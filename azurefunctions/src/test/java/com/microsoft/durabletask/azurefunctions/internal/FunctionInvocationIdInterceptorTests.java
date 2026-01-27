// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions.internal;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for FunctionInvocationIdInterceptor.
 */
public class FunctionInvocationIdInterceptorTests {

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

    @Test
    public void constructor_acceptsEmptyString() {
        // Act & Assert - no exception should be thrown
        FunctionInvocationIdInterceptor interceptor = new FunctionInvocationIdInterceptor("");
        assertNotNull(interceptor);
    }

    @Test
    public void constructor_acceptsWhitespaceString() {
        // Act & Assert - no exception should be thrown
        FunctionInvocationIdInterceptor interceptor = new FunctionInvocationIdInterceptor("   ");
        assertNotNull(interceptor);
    }

    @Test
    public void constructor_acceptsUuidFormat() {
        // Act & Assert - no exception should be thrown with UUID format (common invocation ID format)
        FunctionInvocationIdInterceptor interceptor = new FunctionInvocationIdInterceptor("550e8400-e29b-41d4-a716-446655440000");
        assertNotNull(interceptor);
    }
}
