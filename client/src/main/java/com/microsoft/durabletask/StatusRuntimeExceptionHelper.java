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
                return createCancellationException(e, operationName);
            default:
                return createRuntimeException(e, operationName, code);
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
        switch (code) {
            case DEADLINE_EXCEEDED:
                return new TimeoutException(
                        formatMessage(operationName, code, getDescriptionOrDefault(e)));
            case CANCELLED:
                return createCancellationException(e, operationName);
            default:
                return createRuntimeException(e, operationName, code);
        }
    }

    private static CancellationException createCancellationException(
            StatusRuntimeException e, String operationName) {
        CancellationException ce = new CancellationException(
                "The " + operationName + " operation was canceled.");
        ce.initCause(e);
        return ce;
    }

    private static RuntimeException createRuntimeException(
            StatusRuntimeException e, String operationName, Status.Code code) {
        return new RuntimeException(
                formatMessage(operationName, code, getDescriptionOrDefault(e)), e);
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
