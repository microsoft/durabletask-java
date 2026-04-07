// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

/**
 * Utility class to translate gRPC {@link StatusRuntimeException} into SDK-level exceptions.
 * This ensures callers do not need to depend on gRPC types directly.
 *
 * <p>Status code mappings:
 * <ul>
 *   <li>{@code CANCELLED} → {@link CancellationException}</li>
 *   <li>{@code DEADLINE_EXCEEDED} → {@link TimeoutException} (via {@link #toException})</li>
 *   <li>{@code INVALID_ARGUMENT} → {@link IllegalArgumentException}</li>
 *   <li>{@code FAILED_PRECONDITION} → {@link IllegalStateException}</li>
 *   <li>{@code NOT_FOUND} → {@link NoSuchElementException}</li>
 *   <li>{@code UNIMPLEMENTED} → {@link UnsupportedOperationException}</li>
 *   <li>All other codes → {@link RuntimeException}</li>
 * </ul>
 */
final class StatusRuntimeExceptionHelper {

    /**
     * Translates a {@link StatusRuntimeException} into an appropriate SDK-level unchecked exception.
     *
     * @param e the gRPC exception to translate
     * @param operationName the name of the operation that failed, used in exception messages
     * @return a translated RuntimeException (never returns null)
     */
    static RuntimeException toRuntimeException(StatusRuntimeException e, String operationName) {
        Status.Code code = e.getStatus().getCode();
        String message = formatMessage(operationName, code, getDescriptionOrDefault(e));
        switch (code) {
            case CANCELLED:
                return createCancellationException(e, operationName);
            case INVALID_ARGUMENT:
                return new IllegalArgumentException(message, e);
            case FAILED_PRECONDITION:
                return new IllegalStateException(message, e);
            case NOT_FOUND:
                return createNoSuchElementException(e, message);
            case UNIMPLEMENTED:
                return new UnsupportedOperationException(message, e);
            default:
                return new RuntimeException(message, e);
        }
    }

    /**
     * Translates a {@link StatusRuntimeException} into an appropriate SDK-level checked exception
     * for operations that declare {@code throws TimeoutException}.
     * <p>
     * Note: The DEADLINE_EXCEEDED case is included for completeness and future-proofing, even
     * though current call sites handle DEADLINE_EXCEEDED before falling through to this method.
     * This ensures centralized translation if call sites are refactored in the future.
     *
     * @param e the gRPC exception to translate
     * @param operationName the name of the operation that failed, used in exception messages
     * @return a translated Exception (never returns null)
     */
    static Exception toException(StatusRuntimeException e, String operationName) {
        Status.Code code = e.getStatus().getCode();
        String message = formatMessage(operationName, code, getDescriptionOrDefault(e));
        switch (code) {
            case DEADLINE_EXCEEDED:
                return new TimeoutException(message);
            case CANCELLED:
                return createCancellationException(e, operationName);
            case INVALID_ARGUMENT:
                return new IllegalArgumentException(message, e);
            case FAILED_PRECONDITION:
                return new IllegalStateException(message, e);
            case NOT_FOUND:
                return createNoSuchElementException(e, message);
            case UNIMPLEMENTED:
                return new UnsupportedOperationException(message, e);
            default:
                return new RuntimeException(message, e);
        }
    }

    private static CancellationException createCancellationException(
            StatusRuntimeException e, String operationName) {
        CancellationException ce = new CancellationException(
                "The " + operationName + " operation was canceled.");
        ce.initCause(e);
        return ce;
    }

    private static NoSuchElementException createNoSuchElementException(
            StatusRuntimeException e, String message) {
        NoSuchElementException ne = new NoSuchElementException(message);
        ne.initCause(e);
        return ne;
    }

    private static String formatMessage(String operationName, Status.Code code, String description) {
        return "The " + operationName + " operation failed with a " + code + " gRPC status: " + description;
    }

    private static String getDescriptionOrDefault(StatusRuntimeException e) {
        String description = e.getStatus().getDescription();
        return description != null ? description : "(no description)";
    }

    // Cannot be instantiated
    private StatusRuntimeExceptionHelper() {
    }
}
