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
 * Unit tests for {@link FailureDetails}, covering properties, innerFailure,
 * and proto round-trip serialization.
 */
public class FailureDetailsTest {

    @Test
    void constructFromProto_basicFields() {
        TaskFailureDetails proto = TaskFailureDetails.newBuilder()
                .setErrorType("java.lang.IllegalArgumentException")
                .setErrorMessage("bad argument")
                .setStackTrace(StringValue.of("at Foo.bar(Foo.java:10)"))
                .setIsNonRetriable(true)
                .build();

        FailureDetails details = new FailureDetails(proto);

        assertEquals("java.lang.IllegalArgumentException", details.getErrorType());
        assertEquals("bad argument", details.getErrorMessage());
        assertEquals("at Foo.bar(Foo.java:10)", details.getStackTrace());
        assertTrue(details.isNonRetriable());
        assertNull(details.getInnerFailure());
        assertNull(details.getProperties());
    }

    @Test
    void constructFromProto_withInnerFailure() {
        TaskFailureDetails innerProto = TaskFailureDetails.newBuilder()
                .setErrorType("java.io.IOException")
                .setErrorMessage("disk full")
                .setStackTrace(StringValue.of("at IO.write(IO.java:42)"))
                .build();

        TaskFailureDetails outerProto = TaskFailureDetails.newBuilder()
                .setErrorType("java.lang.RuntimeException")
                .setErrorMessage("operation failed")
                .setStackTrace(StringValue.of("at App.run(App.java:5)"))
                .setInnerFailure(innerProto)
                .build();

        FailureDetails details = new FailureDetails(outerProto);

        assertEquals("java.lang.RuntimeException", details.getErrorType());
        assertNotNull(details.getInnerFailure());

        FailureDetails inner = details.getInnerFailure();
        assertEquals("java.io.IOException", inner.getErrorType());
        assertEquals("disk full", inner.getErrorMessage());
        assertEquals("at IO.write(IO.java:42)", inner.getStackTrace());
        assertNull(inner.getInnerFailure());
    }

    @Test
    void constructFromProto_withNestedInnerFailures() {
        TaskFailureDetails level2 = TaskFailureDetails.newBuilder()
                .setErrorType("java.lang.NullPointerException")
                .setErrorMessage("null ref")
                .setStackTrace(StringValue.of(""))
                .build();

        TaskFailureDetails level1 = TaskFailureDetails.newBuilder()
                .setErrorType("java.io.IOException")
                .setErrorMessage("read error")
                .setStackTrace(StringValue.of(""))
                .setInnerFailure(level2)
                .build();

        TaskFailureDetails level0 = TaskFailureDetails.newBuilder()
                .setErrorType("java.lang.RuntimeException")
                .setErrorMessage("top level")
                .setStackTrace(StringValue.of(""))
                .setInnerFailure(level1)
                .build();

        FailureDetails details = new FailureDetails(level0);

        assertNotNull(details.getInnerFailure());
        assertNotNull(details.getInnerFailure().getInnerFailure());
        assertEquals("java.lang.NullPointerException",
                details.getInnerFailure().getInnerFailure().getErrorType());
        assertNull(details.getInnerFailure().getInnerFailure().getInnerFailure());
    }

    @Test
    void constructFromProto_withProperties_stringValue() {
        TaskFailureDetails proto = TaskFailureDetails.newBuilder()
                .setErrorType("CustomException")
                .setErrorMessage("validation failed")
                .setStackTrace(StringValue.of(""))
                .putProperties("errorCode", Value.newBuilder().setStringValue("VALIDATION_FAILED").build())
                .build();

        FailureDetails details = new FailureDetails(proto);

        assertNotNull(details.getProperties());
        assertEquals(1, details.getProperties().size());
        assertEquals("VALIDATION_FAILED", details.getProperties().get("errorCode"));
    }

    @Test
    void constructFromProto_withProperties_multipleTypes() {
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

        assertNotNull(details.getProperties());
        assertThrows(UnsupportedOperationException.class,
                () -> details.getProperties().put("newKey", "newValue"));
    }

    @Test
    void constructFromException_capturesInnerCause() {
        IOException innerCause = new IOException("disk full");
        RuntimeException outer = new RuntimeException("operation failed", innerCause);

        FailureDetails details = new FailureDetails(outer);

        assertEquals("java.lang.RuntimeException", details.getErrorType());
        assertEquals("operation failed", details.getErrorMessage());
        assertNotNull(details.getStackTrace());
        assertFalse(details.isNonRetriable());

        assertNotNull(details.getInnerFailure());
        FailureDetails inner = details.getInnerFailure();
        assertEquals("java.io.IOException", inner.getErrorType());
        assertEquals("disk full", inner.getErrorMessage());
        assertNull(inner.getInnerFailure());
    }

    @Test
    void constructFromException_deeplyNestedCauses() {
        NullPointerException root = new NullPointerException("null ref");
        IOException mid = new IOException("io error", root);
        RuntimeException top = new RuntimeException("top", mid);

        FailureDetails details = new FailureDetails(top);

        assertNotNull(details.getInnerFailure());
        assertEquals("java.io.IOException", details.getInnerFailure().getErrorType());

        assertNotNull(details.getInnerFailure().getInnerFailure());
        assertEquals("java.lang.NullPointerException",
                details.getInnerFailure().getInnerFailure().getErrorType());

        assertNull(details.getInnerFailure().getInnerFailure().getInnerFailure());
    }

    @Test
    void constructFromException_noCause_innerFailureIsNull() {
        IllegalStateException ex = new IllegalStateException("bad state");

        FailureDetails details = new FailureDetails(ex);

        assertNull(details.getInnerFailure());
        assertNull(details.getProperties());
    }

    @Test
    void constructFromException_nullMessage_defaultsToEmpty() {
        NullPointerException ex = new NullPointerException();

        FailureDetails details = new FailureDetails(ex);

        assertEquals("java.lang.NullPointerException", details.getErrorType());
        assertEquals("", details.getErrorMessage());
    }

    @Test
    void toProto_roundTrip_basicFields() {
        FailureDetails original = new FailureDetails(
                "TestError", "test message", "stack trace", true);

        TaskFailureDetails proto = original.toProto();
        FailureDetails roundTripped = new FailureDetails(proto);

        assertEquals(original.getErrorType(), roundTripped.getErrorType());
        assertEquals(original.getErrorMessage(), roundTripped.getErrorMessage());
        assertEquals(original.getStackTrace(), roundTripped.getStackTrace());
        assertEquals(original.isNonRetriable(), roundTripped.isNonRetriable());
    }

    @Test
    void toProto_roundTrip_withInnerFailure() {
        FailureDetails inner = new FailureDetails(
                "InnerError", "inner msg", "inner stack", false);
        FailureDetails outer = new FailureDetails(
                "OuterError", "outer msg", "outer stack", false, inner, null);

        TaskFailureDetails proto = outer.toProto();
        FailureDetails roundTripped = new FailureDetails(proto);

        assertEquals("OuterError", roundTripped.getErrorType());
        assertNotNull(roundTripped.getInnerFailure());
        assertEquals("InnerError", roundTripped.getInnerFailure().getErrorType());
        assertEquals("inner msg", roundTripped.getInnerFailure().getErrorMessage());
    }

    @Test
    void toProto_roundTrip_withProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("stringProp", "value1");
        properties.put("intProp", 42);
        properties.put("longProp", 999999999L);
        properties.put("boolProp", true);
        properties.put("nullProp", null);

        FailureDetails original = new FailureDetails(
                "TestError", "msg", "stack", false, null, properties);

        TaskFailureDetails proto = original.toProto();
        FailureDetails roundTripped = new FailureDetails(proto);

        assertNotNull(roundTripped.getProperties());
        assertEquals(5, roundTripped.getProperties().size());
        assertEquals("value1", roundTripped.getProperties().get("stringProp"));
        assertEquals(42.0, roundTripped.getProperties().get("intProp"));
        assertEquals(999999999.0, roundTripped.getProperties().get("longProp"));
        assertEquals(true, roundTripped.getProperties().get("boolProp"));
        assertNull(roundTripped.getProperties().get("nullProp"));
    }

    @Test
    void toProto_roundTrip_withInnerFailureAndProperties() {
        Map<String, Object> innerProps = new HashMap<>();
        innerProps.put("errorCode", "DISK_FULL");
        innerProps.put("retryCount", 3);

        FailureDetails inner = new FailureDetails(
                "java.io.IOException", "disk full", "stack", false, null, innerProps);
        FailureDetails outer = new FailureDetails(
                "java.lang.RuntimeException", "operation failed", "outer stack", false, inner, null);

        TaskFailureDetails proto = outer.toProto();
        FailureDetails roundTripped = new FailureDetails(proto);

        assertNull(roundTripped.getProperties());
        assertNotNull(roundTripped.getInnerFailure());

        FailureDetails roundTrippedInner = roundTripped.getInnerFailure();
        assertNotNull(roundTrippedInner.getProperties());
        assertEquals("DISK_FULL", roundTrippedInner.getProperties().get("errorCode"));
        assertEquals(3.0, roundTrippedInner.getProperties().get("retryCount"));
    }

    @Test
    void toProto_noInnerFailure_protoDoesNotHaveInnerFailure() {
        FailureDetails details = new FailureDetails(
                "TestError", "msg", "stack", false);

        TaskFailureDetails proto = details.toProto();

        assertFalse(proto.hasInnerFailure());
        assertTrue(proto.getPropertiesMap().isEmpty());
    }

    @Test
    void toProto_setsIsNonRetriable() {
        FailureDetails details = new FailureDetails(
                "TestError", "msg", "stack", true);

        TaskFailureDetails proto = details.toProto();

        assertTrue(proto.getIsNonRetriable());
    }

    @Test
    void toProto_fromException_roundTripsInnerFailure() {
        IOException cause = new IOException("root cause");
        RuntimeException ex = new RuntimeException("outer", cause);

        FailureDetails details = new FailureDetails(ex);
        TaskFailureDetails proto = details.toProto();
        FailureDetails roundTripped = new FailureDetails(proto);

        assertEquals("java.lang.RuntimeException", roundTripped.getErrorType());
        assertNotNull(roundTripped.getInnerFailure());
        assertEquals("java.io.IOException", roundTripped.getInnerFailure().getErrorType());
        assertEquals("root cause", roundTripped.getInnerFailure().getErrorMessage());
    }

    @Test
    void toString_returnsTypeAndMessage() {
        FailureDetails details = new FailureDetails(
                "java.lang.RuntimeException", "something went wrong", null, false);

        assertEquals("java.lang.RuntimeException: something went wrong", details.toString());
    }

    @Test
    void fourArgConstructor_backwardCompatible() {
        FailureDetails details = new FailureDetails(
                "ErrorType", "message", "stack", true);

        assertEquals("ErrorType", details.getErrorType());
        assertEquals("message", details.getErrorMessage());
        assertEquals("stack", details.getStackTrace());
        assertTrue(details.isNonRetriable());
        assertNull(details.getInnerFailure());
        assertNull(details.getProperties());
    }

    @Test
    void constructFromProto_withInnerFailureAndProperties() {
        TaskFailureDetails innerProto = TaskFailureDetails.newBuilder()
                .setErrorType("java.io.IOException")
                .setErrorMessage("disk full")
                .setStackTrace(StringValue.of(""))
                .putProperties("retryable", Value.newBuilder().setBoolValue(false).build())
                .build();

        TaskFailureDetails outerProto = TaskFailureDetails.newBuilder()
                .setErrorType("java.lang.RuntimeException")
                .setErrorMessage("wrapped")
                .setStackTrace(StringValue.of(""))
                .setInnerFailure(innerProto)
                .putProperties("correlationId", Value.newBuilder().setStringValue("abc-123").build())
                .build();

        FailureDetails details = new FailureDetails(outerProto);

        assertNotNull(details.getProperties());
        assertEquals("abc-123", details.getProperties().get("correlationId"));

        assertNotNull(details.getInnerFailure());
        assertNotNull(details.getInnerFailure().getProperties());
        assertEquals(false, details.getInnerFailure().getProperties().get("retryable"));
    }

    @Test
    void fromException_withProvider_extractsProperties() {
        ExceptionPropertiesProvider provider = exception -> {
            if (exception instanceof IllegalArgumentException) {
                Map<String, Object> props = new HashMap<>();
                props.put("paramName", exception.getMessage());
                props.put("severity", 3);
                props.put("isCritical", true);
                return props;
            }
            return null;
        };

        IllegalArgumentException ex = new IllegalArgumentException("userId");

        FailureDetails details = FailureDetails.fromException(ex, provider);

        assertEquals("java.lang.IllegalArgumentException", details.getErrorType());
        assertEquals("userId", details.getErrorMessage());
        assertNotNull(details.getProperties());
        assertEquals(3, details.getProperties().size());
        assertEquals("userId", details.getProperties().get("paramName"));
        assertEquals(3, details.getProperties().get("severity"));
        assertEquals(true, details.getProperties().get("isCritical"));
    }

    @Test
    void fromException_withProvider_propertiesOnInnerCauseToo() {
        ExceptionPropertiesProvider provider = exception -> {
            Map<String, Object> props = new HashMap<>();
            props.put("exceptionType", exception.getClass().getSimpleName());
            return props;
        };

        IOException inner = new IOException("disk full");
        RuntimeException outer = new RuntimeException("failed", inner);

        FailureDetails details = FailureDetails.fromException(outer, provider);

        assertNotNull(details.getProperties());
        assertEquals("RuntimeException", details.getProperties().get("exceptionType"));

        assertNotNull(details.getInnerFailure());
        assertNotNull(details.getInnerFailure().getProperties());
        assertEquals("IOException", details.getInnerFailure().getProperties().get("exceptionType"));
    }

    @Test
    void fromException_withProvider_returnsNull_noProperties() {
        ExceptionPropertiesProvider provider = exception -> null;

        RuntimeException ex = new RuntimeException("test");

        FailureDetails details = FailureDetails.fromException(ex, provider);

        assertNull(details.getProperties());
    }

    @Test
    void fromException_withNullProvider_noProperties() {
        RuntimeException ex = new RuntimeException("test");

        FailureDetails details = FailureDetails.fromException(ex, null);

        assertEquals("java.lang.RuntimeException", details.getErrorType());
        assertEquals("test", details.getErrorMessage());
        assertNull(details.getProperties());
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
    void fromException_withProvider_roundTripsViaProto() {
        ExceptionPropertiesProvider provider = exception -> {
            Map<String, Object> props = new HashMap<>();
            props.put("errorCode", "VALIDATION_FAILED");
            props.put("retryCount", 3);
            props.put("isCritical", true);
            return props;
        };

        IllegalArgumentException ex = new IllegalArgumentException("bad input");
        FailureDetails details = FailureDetails.fromException(ex, provider);

        TaskFailureDetails proto = details.toProto();
        FailureDetails roundTripped = new FailureDetails(proto);

        assertNotNull(roundTripped.getProperties());
        assertEquals("VALIDATION_FAILED", roundTripped.getProperties().get("errorCode"));
        assertEquals(3.0, roundTripped.getProperties().get("retryCount"));
        assertEquals(true, roundTripped.getProperties().get("isCritical"));
    }

    @Test
    void constructFromProto_withProperties_containsNullKey() {
        // Properties map with a null-valued entry should be preserved
        TaskFailureDetails proto = TaskFailureDetails.newBuilder()
                .setErrorType("CustomException")
                .setErrorMessage("msg")
                .setStackTrace(StringValue.of(""))
                .putProperties("presentKey", Value.newBuilder().setStringValue("value").build())
                .putProperties("nullKey", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .build();

        FailureDetails details = new FailureDetails(proto);

        assertNotNull(details.getProperties());
        assertEquals(2, details.getProperties().size());
        assertTrue(details.getProperties().containsKey("nullKey"));
        assertNull(details.getProperties().get("nullKey"));
        assertEquals("value", details.getProperties().get("presentKey"));
    }
}
