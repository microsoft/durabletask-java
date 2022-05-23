// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Functional interface for implementing custom task retry handlers.
 * <p>
 * It's important to remember that retry handler code is an extension of the orchestrator code and must therefore comply
 * with all the determinism requirements of orchestrator code.
 */
@FunctionalInterface
public interface RetryHandler {
    /**
     * Invokes the retry handler logic and returns a value indicating whether to continue retrying.
     *
     * @param context retry context that's updated between each retry attempt
     * @return {@code true} to continue retrying or {@code false} to stop retrying.
     */
    boolean handle(RetryContext context);
}
