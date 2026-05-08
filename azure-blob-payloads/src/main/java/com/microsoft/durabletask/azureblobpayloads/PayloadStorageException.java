// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

/**
 * Exception thrown when a payload storage operation fails permanently.
 * <p>
 * This includes scenarios such as payload exceeding the configured maximum size,
 * blob not found during download, or permanent authentication/authorization failures (4xx HTTP errors
 * excluding 408 Request Timeout and 429 Too Many Requests which are retried automatically).
 */
public final class PayloadStorageException extends IllegalStateException {

    /**
     * Creates a new {@code PayloadStorageException} with the specified message.
     *
     * @param message the error message
     */
    public PayloadStorageException(String message) {
        super(message);
    }

    /**
     * Creates a new {@code PayloadStorageException} with the specified message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public PayloadStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
