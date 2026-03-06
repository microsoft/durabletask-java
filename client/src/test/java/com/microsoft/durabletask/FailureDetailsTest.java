// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import com.google.protobuf.NullValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Value;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TaskFailureDetails;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link FailureDetails} proto serialization and provider logic.
 */
public class FailureDetailsTest {

    @Test
    void constructFromProto_withInnerFailureAndProperties() {
        TaskFailureDetails innerProto = TaskFailureDetails.newBuilder()
                .setErrorType("java.io.IOException")
                .setErrorMessage("disk full")
                .setStackTrace(StringValue.of("at IO.write(IO.java:42)"))
                .putProperties("retryable", Value.newBuilder().setBoolValue(false).build())
                .build();

        TaskFailureDetails outerProto = TaskFailureDetails.newBuilder()
                .setErrorType("java.lang.RuntimeException")
                .setErrorMessage("operation failed")
                .setStackTrace(StringValue.of("at App.run(App.java:5)"))
                .setInnerFailure(innerProto)
                .putProperties("correlationId", Value.newBuilder().setStringValue("abc-123").build())
                .build();

        FailureDetails details = new FailureDetails(outerProto);

        assertEquals("java.lang.RuntimeException", details.getErrorType());
        assertEquals("operation failed", details.getErrorMessage());
        assertNotNull(details.getProperties());
        assertEquals("abc-123", details.getProperties().get("correlationId"));

        assertNotNull(details.getInnerFailure());
        FailureDetails inner = details.getInnerFailure();
        assertEquals("java.io.IOException", inner.getErrorType());
        assertEquals("disk full", inner.getErrorMessage());
        assertNotNull(inner.getProperties());
        assertEquals(false, inner.getProperties().get("retryable"));
        assertNull(inner.getInnerFailure());
    }

    @Test
    void constructFromProto_multiplePropertyTypes() {
        TaskFailureDetails proto = TaskFailureDetails.newBuilder()
                .setErrorType("CustomException")
                .setErrorMessage("error")
                .setStackTrace(StringValue.of(""))
                .putProperties("stringProp", Value.newBuilder().setStringValue("hello").build())
                .putProperties("intProp", Value.newBuilder().setNumberValue(100.0).build())
                .putProperties("boolProp", Value.newBuilder().setBoolValue(true).build())
                .putProperties("nullProp", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("longProp", Value.newBuilder().setNumberValue(999999999.0).build())
                .build();

        FailureDetails details = new FailureDetails(proto);

        assertNotNull(details.getProperties());
        assertEquals(5, details.getProperties().size());
        assertEquals("hello", details.getProperties().get("stringProp"));
        assertEquals(100.0, details.getProperties().get("intProp"));
        assertEquals(true, details.getProperties().get("boolProp"));
        assertNull(details.getProperties().get("nullProp"));
        assertTrue(details.getProperties().containsKey("nullProp"));
        assertEquals(999999999.0, details.getProperties().get("longProp"));
    }

    @Test
    void constructFromProto_emptyProperties_returnsNull() {
        TaskFailureDetails proto = TaskFailureDetails.newBuilder()
                .setErrorType("SomeError")
                .setErrorMessage("msg")
                .setStackTrace(StringValue.of(""))
                .build();

        FailureDetails details = new FailureDetails(proto);

        assertNull(details.getProperties());
        assertNull(details.getInnerFailure());
    }

    @Test
    void constructFromProto_propertiesAreUnmodifiable() {
        TaskFailureDetails proto = TaskFailureDetails.newBuilder()
                .setErrorType("SomeError")
                .setErrorMessage("msg")
                .setStackTrace(StringValue.of(""))
                .putProperties("key", Value.newBuilder().setStringValue("value").build())
                .build();

        FailureDetails details = new FailureDetails(proto);

        assertThrows(UnsupportedOperationException.class,
                () -> details.getProperties().put("newKey", "newValue"));
    }

    @Test
    void toProto_roundTrip_withInnerFailureAndProperties() {
        Map<String, Object> innerProps = new HashMap<>();
        innerProps.put("errorCode", "DISK_FULL");
        innerProps.put("retryCount", 3);
        innerProps.put("isCritical", true);
        innerProps.put("nullVal", null);

        FailureDetails inner = new FailureDetails(
                "java.io.IOException", "disk full", "stack", false, null, innerProps);
        FailureDetails outer = new FailureDetails(
                "java.lang.RuntimeException", "operation failed", "outer stack", true, inner, null);

        TaskFailureDetails proto = outer.toProto();
        FailureDetails roundTripped = new FailureDetails(proto);

        assertEquals("java.lang.RuntimeException", roundTripped.getErrorType());
        assertTrue(roundTripped.isNonRetriable());
        assertNull(roundTripped.getProperties());

        assertNotNull(roundTripped.getInnerFailure());
        FailureDetails roundTrippedInner = roundTripped.getInnerFailure();
        assertEquals("java.io.IOException", roundTrippedInner.getErrorType());
        assertNotNull(roundTrippedInner.getProperties());
        assertEquals(4, roundTrippedInner.getProperties().size());
        assertEquals("DISK_FULL", roundTrippedInner.getProperties().get("errorCode"));
        assertEquals(3.0, roundTrippedInner.getProperties().get("retryCount"));
        assertEquals(true, roundTrippedInner.getProperties().get("isCritical"));
        assertTrue(roundTrippedInner.getProperties().containsKey("nullVal"));
        assertNull(roundTrippedInner.getProperties().get("nullVal"));
    }

    @Test
    void fromException_withProvider_extractsAndRoundTrips() {
        ExceptionPropertiesProvider provider = exception -> {
            Map<String, Object> props = new HashMap<>();
            props.put("exceptionType", exception.getClass().getSimpleName());
            return props;
        };

        IOException inner = new IOException("disk full");
        RuntimeException outer = new RuntimeException("failed", inner);

        FailureDetails details = FailureDetails.fromException(outer, provider);

        // Provider called on outer
        assertNotNull(details.getProperties());
        assertEquals("RuntimeException", details.getProperties().get("exceptionType"));

        // Provider called recursively on inner
        assertNotNull(details.getInnerFailure());
        assertNotNull(details.getInnerFailure().getProperties());
        assertEquals("IOException", details.getInnerFailure().getProperties().get("exceptionType"));

        // Round-trip through proto preserves everything
        TaskFailureDetails proto = details.toProto();
        FailureDetails roundTripped = new FailureDetails(proto);

        assertEquals("java.lang.RuntimeException", roundTripped.getErrorType());
        assertEquals("RuntimeException", roundTripped.getProperties().get("exceptionType"));
        assertEquals("java.io.IOException", roundTripped.getInnerFailure().getErrorType());
        assertEquals("IOException", roundTripped.getInnerFailure().getProperties().get("exceptionType"));
    }

    @Test
    void fromException_withNullProvider_noProperties() {
        RuntimeException ex = new RuntimeException("test", new IOException("cause"));

        FailureDetails details = FailureDetails.fromException(ex, null);

        assertNull(details.getProperties());
        assertNotNull(details.getInnerFailure());
        assertNull(details.getInnerFailure().getProperties());
    }

    @Test
    void fromException_providerThrows_gracefullyIgnored() {
        ExceptionPropertiesProvider provider = exception -> {
            throw new RuntimeException("provider error");
        };

        IllegalStateException ex = new IllegalStateException("original error");

        FailureDetails details = FailureDetails.fromException(ex, provider);

        assertEquals("java.lang.IllegalStateException", details.getErrorType());
        assertEquals("original error", details.getErrorMessage());
        assertNull(details.getProperties());
    }

    @Test
    void fromException_providerReturnsNull_noProperties() {
        ExceptionPropertiesProvider provider = exception -> null;

        FailureDetails details = FailureDetails.fromException(new RuntimeException("test"), provider);

        assertNull(details.getProperties());
    }

    @Test
    void fromException_providerSelectivelyReturnsProperties() {
        ExceptionPropertiesProvider provider = exception -> {
            if (exception instanceof IllegalArgumentException) {
                Map<String, Object> props = new HashMap<>();
                props.put("paramName", "userId");
                return props;
            }
            return null;
        };

        IllegalArgumentException inner = new IllegalArgumentException("bad param");
        RuntimeException outer = new RuntimeException("wrapper", inner);

        FailureDetails details = FailureDetails.fromException(outer, provider);

        // Provider returns null for RuntimeException
        assertNull(details.getProperties());

        // Provider returns properties for IllegalArgumentException
        assertNotNull(details.getInnerFailure());
        assertNotNull(details.getInnerFailure().getProperties());
        assertEquals("userId", details.getInnerFailure().getProperties().get("paramName"));
    }
}
