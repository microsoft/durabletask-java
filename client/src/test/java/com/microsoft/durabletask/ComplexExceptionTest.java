// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for handling complex exception serialization scenarios.
 */
public class ComplexExceptionTest {

    @Test
    public void testDeepNestedExceptions() {
        // Create a chain of 5 nested exceptions
        Exception level5 = new IllegalArgumentException("Level 5 exception");
        Exception level4 = new IllegalStateException("Level 4 exception", level5);
        Exception level3 = new RuntimeException("Level 3 exception", level4);
        Exception level2 = new Exception("Level 2 exception", level3);
        Exception level1 = new Exception("Level 1 exception", level2);
        
        FailureDetails details = new FailureDetails(level1);
        
        assertEquals("java.lang.Exception", details.getErrorType());
        assertEquals("Level 1 exception", details.getErrorMessage());
        
        String stackTrace = details.getStackTrace();
        assertNotNull(stackTrace);
        
        // Verify all exception levels are present in the stack trace
        assertTrue(stackTrace.contains("Level 1 exception"));
        assertTrue(stackTrace.contains("Caused by: java.lang.Exception: Level 2 exception"));
        assertTrue(stackTrace.contains("Caused by: java.lang.RuntimeException: Level 3 exception"));
        assertTrue(stackTrace.contains("Caused by: java.lang.IllegalStateException: Level 4 exception"));
        assertTrue(stackTrace.contains("Caused by: java.lang.IllegalArgumentException: Level 5 exception"));
    }
    
    @Test
    public void testExceptionWithSuppressedExceptions() {
        Exception mainException = new RuntimeException("Main exception");
        Exception suppressed1 = new IllegalArgumentException("Suppressed exception 1");
        Exception suppressed2 = new IllegalStateException("Suppressed exception 2");
        
        mainException.addSuppressed(suppressed1);
        mainException.addSuppressed(suppressed2);
        
        FailureDetails details = new FailureDetails(mainException);
        
        assertEquals("java.lang.RuntimeException", details.getErrorType());
        assertEquals("Main exception", details.getErrorMessage());
        
        String stackTrace = details.getStackTrace();
        assertNotNull(stackTrace);
        
        // Verify suppressed exceptions are in the stack trace
        assertTrue(stackTrace.contains("Main exception"));
        assertTrue(stackTrace.contains("Suppressed: java.lang.IllegalArgumentException: Suppressed exception 1"));
        assertTrue(stackTrace.contains("Suppressed: java.lang.IllegalStateException: Suppressed exception 2"));
    }
    
    @Test
    public void testNullMessageException() {
        NullPointerException exception = new NullPointerException();  // NPE typically has null message
        
        FailureDetails details = new FailureDetails(exception);
        
        assertEquals("java.lang.NullPointerException", details.getErrorType());
        assertEquals("", details.getErrorMessage());  // Should convert null to empty string
        assertNotNull(details.getStackTrace());
    }
    
    @Test
    public void testCircularExceptionReference() {
        try {
            // Create an exception with a circular reference (should be handled gracefully)
            ExceptionWithCircularReference ex = new ExceptionWithCircularReference("Circular");
            ex.setCircularCause();
            
            FailureDetails details = new FailureDetails(ex);
            
            assertEquals(ExceptionWithCircularReference.class.getName(), details.getErrorType());
            assertEquals("Circular", details.getErrorMessage());
            assertNotNull(details.getStackTrace());
            
            // No infinite loop, test passes if we get here
        } catch (StackOverflowError e) {
            fail("StackOverflowError occurred with circular exception reference");
        }
    }
    
    /**
     * Exception class that can create a circular reference in the cause chain.
     */
    private static class ExceptionWithCircularReference extends Exception {
        public ExceptionWithCircularReference(String message) {
            super(message);
        }
        
        public void setCircularCause() {
            try {
                // Use reflection to set the cause field directly to this exception
                // to create a circular reference
                java.lang.reflect.Field causeField = Throwable.class.getDeclaredField("cause");
                causeField.setAccessible(true);
                causeField.set(this, this);
            } catch (Exception e) {
                // Ignore any reflection errors, this is just for testing
            }
        }
    }
}