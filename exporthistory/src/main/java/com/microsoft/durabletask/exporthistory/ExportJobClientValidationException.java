// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

/**
 * Thrown when export job creation options or client arguments fail validation.
 */
public final class ExportJobClientValidationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new {@code ExportJobClientValidationException}.
     *
     * @param message the validation error message
     */
    public ExportJobClientValidationException(String message) {
        super(message);
    }

    /**
     * Creates a new {@code ExportJobClientValidationException} with a cause.
     *
     * @param message the validation error message
     * @param cause   the underlying cause
     */
    public ExportJobClientValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
