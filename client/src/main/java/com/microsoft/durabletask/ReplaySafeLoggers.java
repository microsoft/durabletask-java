// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.slf4j.ILoggerFactory;

/**
 * Utility class for unwrapping nested {@link ReplaySafeLoggerFactory} instances.
 *
 * <p>Mirrors {@code TaskOrchestrationContext.GetUnwrappedLoggerFactory()} in the modern .NET SDK.
 * When a wrapper context delegates {@code getLoggerFactory()} to
 * {@code inner.getReplaySafeLoggerFactory()}, the returned factory is already a
 * {@link ReplaySafeLoggerFactory}. Without unwrapping, loggers produced from it would be
 * double-wrapped with redundant replay-safe checks.
 */
final class ReplaySafeLoggers {

    private static final int MAX_UNWRAP_DEPTH = 10;

    private ReplaySafeLoggers() {
        // utility class
    }

    /**
     * Returns the underlying {@link ILoggerFactory} for the given context, walking past any
     * nested {@link ReplaySafeLoggerFactory} wrappers.
     *
     * @param context the orchestration context whose logger factory to unwrap
     * @return the first non-replay-safe-wrapped factory in the chain
     * @throws IllegalStateException if more than {@value #MAX_UNWRAP_DEPTH} levels of wrapping
     *         are encountered (cycle protection)
     */
    static ILoggerFactory unwrap(TaskOrchestrationContext context) {
        Helpers.throwIfArgumentNull(context, "context");
        ILoggerFactory factory = context.getLoggerFactory();
        int depth = 0;
        while (factory instanceof ReplaySafeLoggerFactory) {
            if (++depth > MAX_UNWRAP_DEPTH) {
                throw new IllegalStateException(
                    "Maximum unwrap depth exceeded while resolving the underlying ILoggerFactory. " +
                    "Ensure the wrapper's getLoggerFactory() delegates to the inner context's " +
                    "getReplaySafeLoggerFactory() (e.g., 'inner.getReplaySafeLoggerFactory()'), " +
                    "not 'this.getReplaySafeLoggerFactory()'.");
            }
            factory = ((ReplaySafeLoggerFactory) factory).underlying();
        }
        return factory;
    }
}
