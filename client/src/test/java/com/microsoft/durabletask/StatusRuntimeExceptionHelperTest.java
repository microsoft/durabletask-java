// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StatusRuntimeExceptionHelper}.
 */
public class StatusRuntimeExceptionHelperTest {

    // Tests for toRuntimeException

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
    void toRuntimeException_unavailableStatus_returnsRuntimeException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("Connection refused"));

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "terminate");

        assertInstanceOf(RuntimeException.class, result);
        assertNotEquals(CancellationException.class, result.getClass());
        assertTrue(result.getMessage().contains("terminate"));
        assertTrue(result.getMessage().contains("UNAVAILABLE"));
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
    void toRuntimeException_deadlineExceededStatus_returnsRuntimeException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(Status.DEADLINE_EXCEEDED);

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "getInstanceMetadata");

        assertInstanceOf(RuntimeException.class, result);
        assertTrue(result.getMessage().contains("getInstanceMetadata"));
        assertTrue(result.getMessage().contains("DEADLINE_EXCEEDED"));
    }

    @Test
    void toRuntimeException_preservesOperationName() {
        StatusRuntimeException grpcException = new StatusRuntimeException(Status.UNKNOWN);

        RuntimeException result = StatusRuntimeExceptionHelper.toRuntimeException(
                grpcException, "customOperationName");

        assertTrue(result.getMessage().contains("customOperationName"));
    }

    // Tests for toException (checked exception variant)

    @Test
    void toException_deadlineExceededStatus_returnsTimeoutException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(
                Status.DEADLINE_EXCEEDED.withDescription("deadline exceeded after 10s"));

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "waitForInstanceStart");

        assertInstanceOf(TimeoutException.class, result);
        assertTrue(result.getMessage().contains("waitForInstanceStart"));
        assertTrue(result.getMessage().contains("timed out"));
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
    void toException_unavailableStatus_returnsRuntimeException() {
        StatusRuntimeException grpcException = new StatusRuntimeException(Status.UNAVAILABLE);

        Exception result = StatusRuntimeExceptionHelper.toException(
                grpcException, "waitForInstanceCompletion");

        assertInstanceOf(RuntimeException.class, result);
        assertNotEquals(CancellationException.class, result.getClass());
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
}
