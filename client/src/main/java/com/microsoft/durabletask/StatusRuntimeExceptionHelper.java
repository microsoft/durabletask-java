// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

/**
 * Utility class to translate gRPC {@link StatusRuntimeException} into SDK-level exceptions.
 * This ensures callers do not need to depend on gRPC types directly.
 */
final class StatusRuntimeExceptionHelper {

    /**
     * Translates a {@link StatusRuntimeException} into an appropriate SDK-level exception.
     *
     * @param e the gRPC exception to translate
     * @param operationName the name of the operation that failed, used in exception messages
     * @return a translated RuntimeException (never returns null)
     */
    static RuntimeException toRuntimeException(StatusRuntimeException e, String operationName) {
        Status.Code code = e.getStatus().getCode();
        switch (code) {
            case CANCELLED:
                CancellationException ce = new CancellationException(
                        "The " + operationName + " operation was canceled.");
                ce.initCause(e);
                return ce;
            default:
                return new RuntimeException(
                        "The " + operationName + " operation failed with a " + code + " gRPC status: "
                                + e.getStatus().getDescription(),
                        e);
        }
    }

    /**
     * Translates a {@link StatusRuntimeException} into an appropriate SDK-level checked exception
     * for operations that declare {@code throws TimeoutException}.
     *
     * @param e the gRPC exception to translate
     * @param operationName the name of the operation that failed, used in exception messages
     * @return a translated Exception (never returns null)
     */
    static Exception toException(StatusRuntimeException e, String operationName) {
        Status.Code code = e.getStatus().getCode();
        switch (code) {
            case DEADLINE_EXCEEDED:
                return new TimeoutException(
                        "The " + operationName + " operation timed out: " + e.getStatus().getDescription());
            case CANCELLED:
                CancellationException ce = new CancellationException(
                        "The " + operationName + " operation was canceled.");
                ce.initCause(e);
                return ce;
            default:
                return new RuntimeException(
                        "The " + operationName + " operation failed with a " + code + " gRPC status: "
                                + e.getStatus().getDescription(),
                        e);
        }
    }

    // Cannot be instantiated
    private StatusRuntimeExceptionHelper() {
    }
}
