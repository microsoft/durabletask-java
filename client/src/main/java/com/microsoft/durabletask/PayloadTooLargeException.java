// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Thrown when a payload exceeds the maximum allowed size for externalization.
 *
 * @see LargePayloadOptions#getMaxExternalizedPayloadBytes()
 */
public class PayloadTooLargeException extends RuntimeException {

    /**
     * Creates a new PayloadTooLargeException with the specified message.
     *
     * @param message the detail message
     */
    public PayloadTooLargeException(String message) {
        super(message);
    }
}
