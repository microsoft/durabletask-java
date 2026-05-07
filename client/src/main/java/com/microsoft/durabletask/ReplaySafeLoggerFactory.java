// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

/**
 * An {@link ILoggerFactory} that produces replay-safe loggers backed by the context's
 * underlying {@link TaskOrchestrationContext#getLoggerFactory() logger factory}.
 *
 * <p>Mirrors the {@code ReplaySafeLoggerFactoryImpl} nested class in the modern .NET
 * {@code TaskOrchestrationContext}.
 */
final class ReplaySafeLoggerFactory implements ILoggerFactory {

    private final TaskOrchestrationContext context;

    ReplaySafeLoggerFactory(TaskOrchestrationContext context) {
        Helpers.throwIfArgumentNull(context, "context");
        this.context = context;
    }

    /**
     * Returns the factory underlying this wrapper. Used by {@link ReplaySafeLoggers#unwrap}
     * to walk past nested replay-safe wrappers (wrapper-context delegation pattern).
     */
    ILoggerFactory underlying() {
        return context.getLoggerFactory();
    }

    @Override
    public Logger getLogger(String name) {
        Helpers.throwIfArgumentNullOrWhiteSpace(name, "name");
        return new ReplaySafeLogger(context, ReplaySafeLoggers.unwrap(context).getLogger(name));
    }
}
