// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for exception serialization and deserialization in FailureDetails.
 */
public class ExceptionSerializationTest {

    @Test
    public void testSimpleException() {
        String message = "Simple exception message";
        RuntimeException exception = new RuntimeException(message);
        FailureDetails details = new FailureDetails(exception);

        assertEquals("java.lang.RuntimeException", details.getErrorType());
        assertEquals(message, details.getErrorMessage());
        assertNotNull(details.getStackTrace());
        assertTrue(details.getStackTrace().contains("at com.microsoft.durabletask.ExceptionSerializationTest"));
    }

    @Test
    public void testMultilineMessage() {
        String multilineMessage = "Line 1\nLine 2\nLine 3";
        RuntimeException exception = new RuntimeException(multilineMessage);
        FailureDetails details = new FailureDetails(exception);

        assertEquals("java.lang.RuntimeException", details.getErrorType());
        assertEquals(multilineMessage, details.getErrorMessage());
        assertTrue(details.getStackTrace().contains("at com.microsoft.durabletask.ExceptionSerializationTest"));
    }

    @Test
    public void testNestedException() {
        String innerMessage = "Inner exception";
        String outerMessage = "Outer exception";
        
        Exception innerException = new IllegalArgumentException(innerMessage);
        RuntimeException outerException = new RuntimeException(outerMessage, innerException);
        
        FailureDetails details = new FailureDetails(outerException);
        
        assertEquals("java.lang.RuntimeException", details.getErrorType());
        assertEquals(outerMessage, details.getErrorMessage());
        
        String stackTrace = details.getStackTrace();
        assertNotNull(stackTrace);
        
        // The stack trace should contain information about both exceptions
        assertTrue(stackTrace.contains(outerMessage), "Stack trace should contain outer exception message");
        assertTrue(stackTrace.contains(innerMessage), "Stack trace should contain inner exception message");
        assertTrue(stackTrace.contains("Caused by: java.lang.IllegalArgumentException"), 
                "Stack trace should include 'Caused by' section");
    }

    @Test
    public void testNonStandardExceptionToString() {
        CustomException exception = new CustomException("Custom message");
        FailureDetails details = new FailureDetails(exception);
        
        assertEquals(CustomException.class.getName(), details.getErrorType());
        assertEquals("Custom message", details.getErrorMessage());
        assertNotNull(details.getStackTrace());
    }
    
    /**
     * Custom exception class with a non-standard toString implementation.
     */
    private static class CustomException extends Exception {
        public CustomException(String message) {
            super(message);
        }
        
        @Override
        public String toString() {
            return "CUSTOM_EXCEPTION_FORMAT: " + getMessage();
        }
    }
}