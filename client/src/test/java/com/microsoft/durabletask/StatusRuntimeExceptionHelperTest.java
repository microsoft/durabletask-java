// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StatusRuntimeExceptionHelper}.
 */
public class StatusRuntimeExceptionHelperTest {

    // -- toRuntimeException tests --

    @Test
    void toRuntimeException_cancelledStatus_returnsCancellationException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(Status.CANCELLED);

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "testOperation");

        assertInstanceOf(CancellationException.class, result);
        assertTrue(result.getMessage().contains("testOperation"));
        assertTrue(result.getMessage().contains("canceled"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toRuntimeException_cancelledStatusWithDescription_returnsCancellationException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.CANCELLED.withDescription("context cancelled"));

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "raiseEvent");

        assertInstanceOf(CancellationException.class, result);
        assertTrue(result.getMessage().contains("raiseEvent"));
    }

    @Test
    void toRuntimeException_invalidArgumentStatus_returnsIllegalArgumentException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription("instanceId is required"));

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "scheduleNewOrchestrationInstance");

        assertInstanceOf(IllegalArgumentException.class, result);
        assertTrue(result.getMessage().contains("scheduleNewOrchestrationInstance"));
        assertTrue(result.getMessage().contains("INVALID_ARGUMENT"));
        assertTrue(result.getMessage().contains("instanceId is required"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toRuntimeException_failedPreconditionStatus_returnsIllegalStateException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.FAILED_PRECONDITION.withDescription("instance already running"));

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "suspendInstance");

        assertInstanceOf(IllegalStateException.class, result);
        assertTrue(result.getMessage().contains("suspendInstance"));
        assertTrue(result.getMessage().contains("FAILED_PRECONDITION"));
        assertTrue(result.getMessage().contains("instance already running"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toRuntimeException_notFoundStatus_returnsNoSuchElementException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.NOT_FOUND.withDescription("instance not found"));

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "getInstanceMetadata");

        assertInstanceOf(NoSuchElementException.class, result);
        assertTrue(result.getMessage().contains("getInstanceMetadata"));
        assertTrue(result.getMessage().contains("NOT_FOUND"));
        assertTrue(result.getMessage().contains("instance not found"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toRuntimeException_unimplementedStatus_returnsUnsupportedOperationException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.UNIMPLEMENTED.withDescription("method not supported"));

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "rewindInstance");

        assertInstanceOf(UnsupportedOperationException.class, result);
        assertTrue(result.getMessage().contains("rewindInstance"));
        assertTrue(result.getMessage().contains("UNIMPLEMENTED"));
        assertTrue(result.getMessage().contains("method not supported"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toRuntimeException_deadlineExceededStatus_returnsRuntimeException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(Status.DEADLINE_EXCEEDED);

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "getInstanceMetadata");

        assertInstanceOf(RuntimeException.class, result);
        assertTrue(result.getMessage().contains("getInstanceMetadata"));
        assertTrue(result.getMessage().contains("DEADLINE_EXCEEDED"));
    }

    @Test
    void toRuntimeException_unavailableStatus_returnsRuntimeException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("Connection refused"));

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "terminate");

        assertInstanceOf(RuntimeException.class, result);
        assertFalse(result instanceof IllegalArgumentException);
        assertFalse(result instanceof IllegalStateException);
        assertFalse(result instanceof UnsupportedOperationException);
        assertTrue(result.getMessage().contains("terminate"));
        assertTrue(result.getMessage().contains("UNAVAILABLE"));
        assertTrue(result.getMessage().contains("Connection refused"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toRuntimeException_internalStatus_returnsRuntimeException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.INTERNAL.withDescription("Internal server error"));

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "suspendInstance");

        assertInstanceOf(RuntimeException.class, result);
        assertTrue(result.getMessage().contains("suspendInstance"));
        assertTrue(result.getMessage().contains("INTERNAL"));
        assertTrue(result.getMessage().contains("Internal server error"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toRuntimeException_preservesOperationName() {
        StatusRuntimeException grpcException = new StatusRuntimeException(Status.UNKNOWN);

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "customOperationName");

        assertTrue(result.getMessage().contains("customOperationName"));
    }

    @Test
    void toRuntimeException_nullDescription_usesDefaultFallback() {
        StatusRuntimeException grpcException = new StatusRuntimeException(Status.INTERNAL);

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "testOp");

        assertTrue(result.getMessage().contains("(no description)"),
                "Expected '(no description)' fallback but got: " + result.getMessage());
        assertFalse(result.getMessage().contains(": null"),
                "Message should not contain literal ': null': " + result.getMessage());
    }

    // -- toException tests --

    @Test
    void toException_deadlineExceededStatus_returnsTimeoutException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.DEADLINE_EXCEEDED.withDescription("deadline exceeded after 10s"));

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "waitForInstanceStart");

        assertInstanceOf(TimeoutException.class, result);
        assertTrue(result.getMessage().contains("waitForInstanceStart"));
        assertTrue(result.getMessage().contains("DEADLINE_EXCEEDED"));
    }

    @Test
    void toException_deadlineExceededStatus_usesConsistentMessageFormat() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.DEADLINE_EXCEEDED.withDescription("timeout after 5s"));

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "purgeInstances");

        assertInstanceOf(TimeoutException.class, result);
        assertTrue(result.getMessage().contains("failed with a DEADLINE_EXCEEDED gRPC status"),
                "Expected consistent message format but got: " + result.getMessage());
    }

    @Test
    void toException_cancelledStatus_returnsCancellationException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(Status.CANCELLED);

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "purgeInstances");

        assertInstanceOf(CancellationException.class, result);
        assertTrue(result.getMessage().contains("purgeInstances"));
        assertTrue(result.getMessage().contains("canceled"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toException_invalidArgumentStatus_returnsIllegalArgumentException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription("bad input"));

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "waitForInstanceStart");

        assertInstanceOf(IllegalArgumentException.class, result);
        assertTrue(result.getMessage().contains("waitForInstanceStart"));
        assertTrue(result.getMessage().contains("INVALID_ARGUMENT"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toException_failedPreconditionStatus_returnsIllegalStateException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.FAILED_PRECONDITION.withDescription("not ready"));

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "waitForInstanceCompletion");

        assertInstanceOf(IllegalStateException.class, result);
        assertTrue(result.getMessage().contains("waitForInstanceCompletion"));
        assertTrue(result.getMessage().contains("FAILED_PRECONDITION"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toException_notFoundStatus_returnsNoSuchElementException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.NOT_FOUND.withDescription("not found"));

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "purgeInstances");

        assertInstanceOf(NoSuchElementException.class, result);
        assertTrue(result.getMessage().contains("purgeInstances"));
        assertTrue(result.getMessage().contains("NOT_FOUND"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toException_unimplementedStatus_returnsUnsupportedOperationException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.UNIMPLEMENTED.withDescription("not implemented"));

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "rewindInstance");

        assertInstanceOf(UnsupportedOperationException.class, result);
        assertTrue(result.getMessage().contains("rewindInstance"));
        assertTrue(result.getMessage().contains("UNIMPLEMENTED"));
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toException_unavailableStatus_returnsRuntimeException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(Status.UNAVAILABLE);

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "waitForInstanceCompletion");

        assertInstanceOf(RuntimeException.class, result);
        assertFalse(result instanceof CancellationException);
        assertTrue(result.getMessage().contains("waitForInstanceCompletion"));
        assertTrue(result.getMessage().contains("UNAVAILABLE"));
    }

    @Test
    void toException_internalStatus_returnsRuntimeExceptionWithCause() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.INTERNAL.withDescription("server error"));

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "purgeInstances");

        assertInstanceOf(RuntimeException.class, result);
        assertSame(grpcException, result.getCause());
    }

    @Test
    void toException_nullDescription_usesDefaultFallback() {
        StatusRuntimeException grpcException = new StatusRuntimeException(Status.DEADLINE_EXCEEDED);

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "testOp");

        assertInstanceOf(TimeoutException.class, result);
        assertTrue(result.getMessage().contains("(no description)"),
                "Expected '(no description)' fallback but got: " + result.getMessage());
        assertFalse(result.getMessage().contains(": null"),
                "Message should not contain literal ': null': " + result.getMessage());
    }
}
